def is_num_servers_valid(num_servers) -> bool:
    """Check if number of servers is valid (1-5)"""
    return isinstance(num_servers, int) and 1 <= num_servers <= 5


def is_server_id_valid(server_id) -> bool:
    """Check if server ID is valid (0-4)"""
    return isinstance(server_id, int) and 0 <= server_id <= 4
