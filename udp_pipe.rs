use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use parking_lot::{Mutex, RwLock};
use tokio::net::UdpSocket;

use crate::pipe::config::{PipeConfig, UdpPipeConfig};
use crate::pipe::{DEFAULT_ADDRESS_V4, DEFAULT_ADDRESS_V6};
use crate::route::{Index, RouteKey};
use crate::socket::{bind_udp, LocalInterface};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Model {
    High,
    Low,
}
impl Model {
    pub fn is_low(&self) -> bool {
        self == &Model::Low
    }
    pub fn is_high(&self) -> bool {
        self == &Model::High
    }
}
impl Default for Model {
    fn default() -> Self {
        Model::Low
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub enum UDPIndex {
    MainV4(usize),
    MainV6(usize),
    SubV4(usize),
}

impl UDPIndex {
    pub(crate) fn index(&self) -> usize {
        match self {
            UDPIndex::MainV4(i) => *i,
            UDPIndex::MainV6(i) => *i,
            UDPIndex::SubV4(i) => *i,
        }
    }
}

/// initialize udp pipe by config
pub fn udp_pipe(config: UdpPipeConfig) -> anyhow::Result<UdpPipe> {
    config.check()?;
    let mut udp_ports = config.udp_ports;
    udp_ports.resize(config.main_pipeline_num, 0);
    let mut main_udp_v4: Vec<Arc<UdpSocket>> = Vec::with_capacity(config.main_pipeline_num);
    let mut main_udp_v6: Vec<Arc<UdpSocket>> = Vec::with_capacity(config.main_pipeline_num);
    // 因为在mac上v4和v6的对绑定网卡的处理不同，所以这里分开监听，并且分开监听更容易处理发送目标为v4的情况，因为双协议栈下发送v4目标需要转换成v6
    for port in &udp_ports {
        loop {
            let mut addr_v4 = DEFAULT_ADDRESS_V4;
            addr_v4.set_port(*port);
            let socket_v4 = bind_udp(addr_v4, config.default_interface.as_ref())?;
            let udp_v4: std::net::UdpSocket = socket_v4.into();
            if config.use_v6 {
                let mut addr_v6 = DEFAULT_ADDRESS_V6;
                let socket_v6 = if *port == 0 {
                    let port = udp_v4.local_addr()?.port();
                    addr_v6.set_port(port);
                    match bind_udp(addr_v6, config.default_interface.as_ref()) {
                        Ok(socket_v6) => socket_v6,
                        Err(_) => continue,
                    }
                } else {
                    addr_v6.set_port(*port);
                    bind_udp(addr_v6, config.default_interface.as_ref())?
                };
                let udp_v6: std::net::UdpSocket = socket_v6.into();
                main_udp_v6.push(Arc::new(UdpSocket::from_std(udp_v6)?))
            }
            main_udp_v4.push(Arc::new(UdpSocket::from_std(udp_v4)?));
            break;
        }
    }
    let (pipe_line_sender, pipe_line_receiver) = tokio::sync::mpsc::unbounded_channel();
    let socket_layer = Arc::new(SocketLayer {
        main_udp_v4,
        main_udp_v6,
        sub_udp: RwLock::new(Vec::with_capacity(config.sub_pipeline_num)),
        sub_close_notify: Default::default(),
        pipe_line_sender,
        sub_udp_num: config.sub_pipeline_num,
        default_interface: config.default_interface,
    });
    let udp_pipe = UdpPipe {
        pipe_line_receiver,
        socket_layer,
    };
    udp_pipe.init()?;
    udp_pipe.socket_layer.switch_model(config.model)?;
    Ok(udp_pipe)
}

pub struct SocketLayer {
    main_udp_v4: Vec<Arc<UdpSocket>>,
    main_udp_v6: Vec<Arc<UdpSocket>>,
    sub_udp: RwLock<Vec<Arc<UdpSocket>>>,
    sub_close_notify: Mutex<Option<tokio::sync::broadcast::Sender<()>>>,
    pipe_line_sender: tokio::sync::mpsc::UnboundedSender<UdpPipeLine>,
    sub_udp_num: usize,
    default_interface: Option<LocalInterface>,
}

impl SocketLayer {
    pub(crate) fn try_sub_send_to_addr_v4(&self, buf: &[u8], addr: SocketAddr) {
        for (i, udp) in self.sub_udp.read().iter().enumerate() {
            if let Err(e) = udp.try_send_to(buf, addr) {
                log::info!("try_sub_send_to_addr_v4: {e:?},{i},{addr}")
            }
        }
    }
    pub(crate) fn try_main_send_to_addr(&self, buf: &[u8], addr: &[SocketAddr]) {
        let len = self.main_pipeline_len();
        for (i, addr) in addr.iter().enumerate() {
            if let Err(e) = self.get(i % len).unwrap().try_send_to_addr(buf, *addr) {
                log::info!("try_main_send_to_addr: {e:?},{i},{addr}")
            }
        }
    }
    pub(crate) fn generate_route_key_from_addr(
        &self,
        index: usize,
        addr: SocketAddr,
    ) -> anyhow::Result<RouteKey> {
        let route_key = if addr.is_ipv4() {
            if index >= self.main_udp_v4.len() {
                return Err(anyhow!(
                    "index out of bounds: the v4 len is {} but the index is {}",
                    self.main_udp_v4.len(),
                    index
                ));
            }
            RouteKey::new(Index::Udp(UDPIndex::MainV4(index)), addr)
        } else {
            if self.main_udp_v6.is_empty() {
                Err(anyhow!("Not support IPV6"))?
            }
            if index >= self.main_udp_v6.len() {
                return Err(anyhow!(
                    "index out of bounds: the v6 len is {} but the index is {}",
                    self.main_udp_v6.len(),
                    index
                ));
            }
            RouteKey::new(Index::Udp(UDPIndex::MainV6(index)), addr)
        };
        Ok(route_key)
    }
    pub(crate) fn switch_low(&self) {
        let mut guard = self.sub_udp.write();
        if guard.is_empty() {
            return;
        }
        guard.clear();
        if let Some(sub_close_notify) = self.sub_close_notify.lock().take() {
            let _ = sub_close_notify.send(());
        }
    }
    pub(crate) fn switch_high(&self) -> anyhow::Result<()> {
        let mut guard = self.sub_udp.write();
        if !guard.is_empty() {
            return Ok(());
        }
        let mut sub_close_notify_guard = self.sub_close_notify.lock();
        if let Some(sender) = sub_close_notify_guard.take() {
            let _ = sender.send(());
        }
        let (sub_close_notify_sender, _sub_close_notify_receiver) =
            tokio::sync::broadcast::channel(2);
        let mut sub_udp_list = Vec::with_capacity(self.sub_udp_num);
        for _ in 0..self.sub_udp_num {
            let udp = bind_udp(DEFAULT_ADDRESS_V4, self.default_interface.as_ref())?;
            let udp: std::net::UdpSocket = udp.into();
            sub_udp_list.push(Arc::new(UdpSocket::from_std(udp)?));
        }
        for (index, udp) in sub_udp_list.iter().enumerate() {
            let udp = udp.clone();
            let udp_pipe_line = UdpPipeLine::sub_new(
                UDPIndex::SubV4(index),
                udp,
                sub_close_notify_sender.subscribe(),
            );
            self.pipe_line_sender.send(udp_pipe_line)?;
        }
        sub_close_notify_guard.replace(sub_close_notify_sender);
        *guard = sub_udp_list;
        Ok(())
    }

    #[inline]
    fn get_sub_udp(&self, index: usize) -> anyhow::Result<Arc<UdpSocket>> {
        self.sub_udp
            .read()
            .get(index)
            .with_context(|| format!("index={index},overflow"))
            .cloned()
    }
    #[inline]
    fn get_udp(&self, udp_index: &UDPIndex) -> anyhow::Result<Arc<UdpSocket>> {
        Ok(match udp_index {
            UDPIndex::MainV4(index) => self
                .main_udp_v4
                .get(*index)
                .ok_or(anyhow!(
                    "index out of bounds: the v4 len is {} but the index is {index}",
                    self.v4_pipeline_len()
                ))?
                .clone(),
            UDPIndex::MainV6(index) => self
                .main_udp_v6
                .get(*index)
                .ok_or(anyhow!(
                    "index out of bounds: the v6 len is {} but the index is {index}",
                    self.v6_pipeline_len()
                ))?
                .clone(),
            UDPIndex::SubV4(index) => self.get_sub_udp(*index)?,
        })
    }

    #[inline]
    fn get_udp_from_route(&self, route_key: &RouteKey) -> anyhow::Result<Arc<UdpSocket>> {
        Ok(match route_key.index() {
            Index::Udp(index) => self.get_udp(&index)?,
            _ => return Err(anyhow!("invalid protocol")),
        })
    }

    #[inline]
    async fn send_to_addr_via_index(
        &self,
        buf: &[u8],
        addr: SocketAddr,
        index: usize,
    ) -> anyhow::Result<()> {
        let key = self.generate_route_key_from_addr(index, addr)?;
        self.send_to(buf, &key).await
    }

    #[inline]
    fn try_send_to_addr_via_index(
        &self,
        buf: &[u8],
        addr: SocketAddr,
        index: usize,
    ) -> anyhow::Result<()> {
        let key = self.generate_route_key_from_addr(index, addr)?;
        self.try_send_to(buf, &key)
    }
}

impl SocketLayer {
    pub fn model(&self) -> Model {
        if self.sub_udp.read().is_empty() {
            Model::Low
        } else {
            Model::High
        }
    }

    #[inline]
    pub fn main_pipeline_len(&self) -> usize {
        self.v4_pipeline_len()
    }
    #[inline]
    pub fn v4_pipeline_len(&self) -> usize {
        self.main_udp_v4.len()
    }
    #[inline]
    pub fn v6_pipeline_len(&self) -> usize {
        self.main_udp_v6.len()
    }

    pub fn switch_model(&self, model: Model) -> anyhow::Result<()> {
        match model {
            Model::High => self.switch_high(),
            Model::Low => {
                self.switch_low();
                Ok(())
            }
        }
    }

    pub fn local_ports(&self) -> anyhow::Result<Vec<u16>> {
        let mut ports = Vec::with_capacity(self.v4_pipeline_len());
        for udp in &self.main_udp_v4 {
            ports.push(udp.local_addr()?.port());
        }
        Ok(ports)
    }
    pub async fn send_to(&self, buf: &[u8], route_key: &RouteKey) -> anyhow::Result<()> {
        self.get_udp_from_route(route_key)?
            .send_to(buf, route_key.addr())
            .await?;
        Ok(())
    }

    pub fn try_send_to(&self, buf: &[u8], route_key: &RouteKey) -> anyhow::Result<()> {
        self.get_udp_from_route(route_key)?
            .try_send_to(buf, route_key.addr())?;
        Ok(())
    }
    pub async fn send_to_addr<A: Into<SocketAddr>>(
        &self,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<()> {
        self.send_to_addr_via_index(buf, addr.into(), 0).await
    }
    pub fn try_send_to_addr<A: Into<SocketAddr>>(&self, buf: &[u8], addr: A) -> anyhow::Result<()> {
        self.try_send_to_addr_via_index(buf, addr.into(), 0)
    }
    pub fn get(&self, index: usize) -> Option<UdpPipeWriterIndex<'_>> {
        if index >= self.main_udp_v4.len() && index >= self.main_udp_v6.len() {
            return None;
        }
        Some(UdpPipeWriterIndex {
            shadow: self,
            index,
        })
    }
}

pub struct UdpPipe {
    pipe_line_receiver: tokio::sync::mpsc::UnboundedReceiver<UdpPipeLine>,
    socket_layer: Arc<SocketLayer>,
}
impl UdpPipe {
    pub(crate) fn init(&self) -> anyhow::Result<()> {
        for (index, udp) in self.socket_layer.main_udp_v4.iter().enumerate() {
            let udp = udp.clone();
            let udp_pipe_line = UdpPipeLine::main_new(UDPIndex::MainV4(index), udp);
            self.socket_layer.pipe_line_sender.send(udp_pipe_line)?;
        }
        for (index, udp) in self.socket_layer.main_udp_v6.iter().enumerate() {
            let udp = udp.clone();
            let udp_pipe_line = UdpPipeLine::main_new(UDPIndex::MainV6(index), udp);
            self.socket_layer.pipe_line_sender.send(udp_pipe_line)?;
        }
        Ok(())
    }
}
impl UdpPipe {
    pub fn new(config: UdpPipeConfig) -> anyhow::Result<UdpPipe> {
        udp_pipe(config)
    }
    pub async fn accept(&mut self) -> anyhow::Result<UdpPipeLine> {
        self.pipe_line_receiver
            .recv()
            .await
            .context("UdpPipe close")
    }

    #[inline]
    pub fn main_pipeline_len(&self) -> usize {
        self.socket_layer.main_pipeline_len()
    }
    #[inline]
    pub fn v4_pipeline_len(&self) -> usize {
        self.socket_layer.v4_pipeline_len()
    }
    #[inline]
    pub fn v6_pipeline_len(&self) -> usize {
        self.socket_layer.v6_pipeline_len()
    }
    pub fn writer_ref(&self) -> UdpPipeWriterRef<'_> {
        UdpPipeWriterRef {
            socket_layer: &self.socket_layer,
        }
    }
}

#[derive(Clone)]
pub struct UdpPipeWriter {
    socket_layer: Arc<SocketLayer>,
}

impl Deref for UdpPipeWriter {
    type Target = SocketLayer;

    fn deref(&self) -> &Self::Target {
        &self.socket_layer
    }
}

pub struct UdpPipeWriterIndex<'a> {
    shadow: &'a SocketLayer,
    index: usize,
}

impl<'a> UdpPipeWriterIndex<'a> {
    pub async fn send_to_addr<A: Into<SocketAddr>>(
        &self,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<()> {
        self.shadow
            .send_to_addr_via_index(buf, addr.into(), self.index)
            .await
    }
    pub fn try_send_to_addr<A: Into<SocketAddr>>(&self, buf: &[u8], addr: A) -> anyhow::Result<()> {
        self.shadow
            .try_send_to_addr_via_index(buf, addr.into(), self.index)
    }
}

pub struct UdpPipeWriterRef<'a> {
    socket_layer: &'a Arc<SocketLayer>,
}

impl<'a> UdpPipeWriterRef<'a> {
    pub fn to_owned(&self) -> UdpPipeWriter {
        UdpPipeWriter {
            socket_layer: self.socket_layer.clone(),
        }
    }
}

impl<'a> Deref for UdpPipeWriterRef<'a> {
    type Target = Arc<SocketLayer>;

    fn deref(&self) -> &Self::Target {
        self.socket_layer
    }
}

pub struct UdpPipeLine {
    index: Index,
    udp: Option<Arc<UdpSocket>>,
    close_notify: Option<tokio::sync::broadcast::Receiver<()>>,
}

impl UdpPipeLine {
    pub(crate) fn sub_new(
        index: UDPIndex,
        udp: Arc<UdpSocket>,
        close_notify: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        Self {
            index: Index::Udp(index),
            udp: Some(udp),
            close_notify: Some(close_notify),
        }
    }
    pub(crate) fn main_new(index: UDPIndex, udp: Arc<UdpSocket>) -> Self {
        Self {
            index: Index::Udp(index),
            udp: Some(udp),
            close_notify: None,
        }
    }
    pub(crate) fn done(&mut self) {
        let _ = self.udp.take();
        let _ = self.close_notify.take();
    }
}
impl UdpPipeLine {
    pub async fn send_to_addr<A: Into<SocketAddr>>(
        &self,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<()> {
        if let Some(udp) = &self.udp {
            udp.send_to(buf, addr.into()).await?;
            Ok(())
        } else {
            Err(anyhow!("closed"))
        }
    }
    pub fn try_send_to_addr<A: Into<SocketAddr>>(&self, buf: &[u8], addr: A) -> anyhow::Result<()> {
        if let Some(udp) = &self.udp {
            udp.try_send_to(buf, addr.into())?;
            Ok(())
        } else {
            Err(anyhow!("closed"))
        }
    }
    pub async fn send_to(&self, buf: &[u8], route_key: &RouteKey) -> anyhow::Result<()> {
        if self.index != route_key.index() {
            Err(anyhow!("mismatch"))?
        }
        if let Some(udp) = &self.udp {
            udp.send_to(buf, route_key.addr()).await?;
            Ok(())
        } else {
            Err(anyhow!("closed"))
        }
    }
    pub async fn readable(&self) -> anyhow::Result<()> {
        if let Some(udp) = &self.udp {
            udp.readable().await?;
            Ok(())
        } else {
            Err(anyhow!("closed"))
        }
    }
    pub async fn recv_from(&mut self, buf: &mut [u8]) -> Option<anyhow::Result<(usize, RouteKey)>> {
        let udp = if let Some(udp) = &self.udp {
            udp
        } else {
            return None;
        };
        if let Some(close_notify) = &mut self.close_notify {
            loop {
                tokio::select! {
                    _=close_notify.recv()=>{
                         self.done();
                         return None;
                    }
                    result=udp.recv_from(buf)=>{
                        match result{
                            Ok((len,addr))=> return Some(Ok((len, RouteKey::new(self.index, addr)))),
                            Err(e)=>{
                                return Some(Err(e.into()))
                            }
                        }
                    }
                }
            }
        } else {
            match udp.recv_from(buf).await {
                Ok((len, addr)) => Some(Ok((len, RouteKey::new(self.index, addr)))),
                Err(e) => Some(Err(e.into())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::pipe::udp_pipe::{Model, UdpPipeLine};

    #[tokio::test]
    pub async fn create_udp_pipe() {
        let config = crate::pipe::config::UdpPipeConfig::default()
            .set_main_pipeline_num(2)
            .set_sub_pipeline_num(10)
            .set_use_v6(false);
        let mut udp_pipe = crate::pipe::udp_pipe::udp_pipe(config).unwrap();
        let mut count = 0;
        while let Ok(rs) = tokio::time::timeout(Duration::from_secs(1), udp_pipe.accept()).await {
            rs.unwrap();
            count += 1;
        }
        assert_eq!(count, 2)
    }
    #[tokio::test]
    pub async fn create_sub_udp_pipe() {
        let config = crate::pipe::config::UdpPipeConfig::default()
            .set_main_pipeline_num(2)
            .set_sub_pipeline_num(10)
            .set_use_v6(false)
            .set_model(Model::High);
        let mut udp_pipe = crate::pipe::udp_pipe::udp_pipe(config).unwrap();
        let mut count = 0;
        let mut join = Vec::new();
        while let Ok(rs) = tokio::time::timeout(Duration::from_secs(1), udp_pipe.accept()).await {
            join.push(tokio::spawn(pipe_line_recv(rs.unwrap())));
            count += 1;
        }
        udp_pipe.writer_ref().switch_low();

        let mut close_pipe_line_count = 0;
        for x in join {
            let rs = tokio::time::timeout(Duration::from_secs(1), x).await;
            match rs {
                Ok(rs) => {
                    if rs.unwrap() {
                        // pipe_line_recv task done
                        close_pipe_line_count += 1;
                    }
                }
                Err(_) => {}
            }
        }
        assert_eq!(count, 12);
        assert_eq!(close_pipe_line_count, 10);
    }
    async fn pipe_line_recv(mut udp_pipe_line: UdpPipeLine) -> bool {
        let mut buf = [0; 1400];
        let rs = udp_pipe_line.recv_from(&mut buf).await;
        // done
        rs.is_none()
    }
}
