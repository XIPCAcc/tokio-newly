use super::{Driver, Handle, TOKEN_UINTR};

use std::io;
use std::io::Read;

impl Handle {
    pub(crate) fn register_uintr_receiver(
        &self,
        receiver: &mut mio::net::UnixStream,
    ) -> io::Result<()> {
        self.registry
            .register(receiver, TOKEN_UINTR, mio::Interest::READABLE)?;
        Ok(())
    }
}

impl Driver {
    pub(crate) fn ensure_uintr_receiver_registered(&mut self, handle: &Handle) {
        if self.uintr_receiver.is_some() {
            return;
        }

        let Ok(receiver) = uintr_core::try_clone_global_receiver() else {
            return;
        };

        let mut receiver = mio::net::UnixStream::from_std(receiver);

        if handle.register_uintr_receiver(&mut receiver).is_ok() {
            self.uintr_receiver = Some(receiver);
        }
    }

    pub(crate) fn process_uintr(&mut self) {
        if !self.consume_uintr_ready() {
            return;
        }

        let Some(receiver) = self.uintr_receiver.as_mut() else {
            return;
        };

        let mut buf = [0; 128];
        #[allow(clippy::unused_io_amount)]
        loop {
            match receiver.read(&mut buf) {
                Ok(0) => panic!("EOF on uintr self-pipe"),
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("Bad read on uintr self-pipe: {e}"),
            }
        }

        uintr_core::process_global_uintr_wakers();
    }

    pub(crate) fn consume_uintr_ready(&mut self) -> bool {
        let ret = self.uintr_ready;
        self.uintr_ready = false;
        ret
    }
}
