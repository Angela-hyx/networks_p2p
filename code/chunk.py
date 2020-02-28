class chunk:
    def __init__(self, chunk_type, file_name, identifier, owners, data):
        self.type = chunk_type
        self.file_name = file_name
        self.identifier = identifier
        self.owners = owners #list of owners
        self.data = data #bytes
    
    @staticmethod
    def create_owned_chunk(file_name, identifier, data):
        return chunk("owned_chunk", file_name, identifier, [], data)
    
    @staticmethod
    def create_alive_chunk(file_name, identifier, owners):
        return chunk("alive_chunk", file_name, identifier, owners, '')
    
    def add_owner(self, owner):
        if (self.type != "alive_chunk"):
            raise Exception("Cannot add owner to non-alive_chunk")
        self.owners.append(owner)
        