def schema_clientes():
    schema = """
        id_cliente BIGINT NOT NULL,
        nombre STRING,
        email STRING,
        ciudad STRING,
        fecha_registro DATE,
        updated_at TIMESTAMP
           """
    return schema