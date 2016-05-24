use rotor;
use rotor_stream;

enum FabricMsgType {
    Get,
    Put,
    PutRemote,
    Bootstrap,
    SyncStart,
    SyncStream,
    SyncAck,
    SyncFin,
}

pub struct Fabric {
}

enum Connection {
    Sender,
    Receiver,
}

impl Fabric {
    fn new() {
        unimplemented!()
    }
}
