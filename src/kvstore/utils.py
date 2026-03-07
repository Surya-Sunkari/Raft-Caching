import configparser


def is_num_servers_valid(num_servers) -> bool:
    """Check if number of servers is valid (1-5)"""
    return isinstance(num_servers, int) and 1 <= num_servers <= 5


def is_server_id_valid(server_id) -> bool:
    """Check if server ID is valid (0-4)"""
    return isinstance(server_id, int) and 0 <= server_id <= 4


def get_active_servers():
    config = configparser.ConfigParser()
    config.read("config.ini")
    active_str = config.get("Servers", "active")  # Gets "0,1,2,3,4"
    active_ids = [int(id.strip()) for id in active_str.split(",")]
    return active_ids


def get_persistent_state_path() -> str:
    """Return the directory where Raft server state should be stored."""
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config.get("Servers", "persistent_state_path", fallback="memory")


def get_cache_config() -> dict:
    config = configparser.ConfigParser()
    config.read("config.ini")
    return {
        "policy": config.get("Cache", "policy", fallback="none"),
        "capacity": config.getint("Cache", "capacity", fallback=0),
        "write_strategy": config.get("Cache", "write_strategy", fallback="write_through"),
    }
