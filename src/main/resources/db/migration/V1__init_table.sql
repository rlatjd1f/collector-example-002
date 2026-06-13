create table protocol (
    protocol_id     bigint auto_increment,
    protocol_name   varchar(100) not null,
    primary key (protocol_id)
);

create table device
(
    device_id       bigint auto_increment,
    protocol_id     bigint not null,
    device_name     varchar(255) not null,
    device_host     varchar(15) not null,
    device_port     smallint unsigned not null,
    primary key (device_id),
    constraint fk_device_protocol foreign key (protocol_id) references protocol (protocol_id)
);

create table modbus_register (
    device_id           bigint,
    register_id         bigint auto_increment not null,
    register_address    int unsigned not null,
    register_count      int unsigned not null,
    data_type           varchar(20) not null,
    description         varchar(500),
    polling_cycle       tinyint unsigned not null,
    primary key (register_id),
    constraint fk_modbus_register_device foreign key (device_id) references device(device_id)
);


create table function_code (
    code_id         bigint auto_increment not null,
    func_code       tinyint unsigned not null,
    func_name       varchar(50) not null,
    memory_area     varchar(50) not null,
    operation_type  varchar(10) not null,
    primary key (code_id),
    constraint uq_func_code unique (func_code)
)