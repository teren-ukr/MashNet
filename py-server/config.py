from dataclasses import dataclass
import uuid

@dataclass
class NodeConfig:
    host: str = '192.168.50.11'  # IP Java-сервера (з Vagrant)
    port: int = 7000
    my_port: int = 8005
    node_id: str = f"NodePy-{str(uuid.uuid4())[:8]}"