create table protocol (
    protocol_id     bigint auto_increment,
    protocol_name   varchar(100) not null,
    primary key (protocol_id)
);

create table device
(
    device_id       bigint auto_increment,
    device_name     varchar(100) not null,
    primary key (device_id)
);

create table device_interface (
    device_id       bigint not null,
    protocol_id     bigint not null,
    interface_id    bigint auto_increment,
    unit_id         tinyint unsigned not null,
    interface_host  varchar(15) not null,
    interface_port  smallint unsigned not null,
    primary key (interface_id),
    constraint fk_device_interface foreign key (device_id) references deviceInterface (device_id),
    constraint fk_protocol_interface foreign key (protocol_id) references protocol (protocol_id)
);

create table checkpoint_enum_master (
    enum_id     bigint auto_increment not null,
    enum_name   varchar(100) not null,
    primary key (enum_id)
);

create table checkpoint_enum_code (
    enum_id     bigint not null,
    enum_code   int not null,
    enum_value  varchar(100),
    primary key (enum_id, enum_code),
    constraint fk_enum_detail_master foreign key (enum_id) references checkpoint_enum_master(enum_id) on delete cascade
);

-- modbus 전용 체크포인트 테이블
create table checkpoint_modbus (
    device_id           bigint,
    checkpoint_id         bigint auto_increment not null,
    checkpoint_address    int unsigned not null,
    checkpoint_count      int unsigned not null,
    data_type           varchar(20) not null,
    data_unit           varchar(20),
    calculate           varchar(10) not null,
    value_type          varchar(20) not null,
    enum_id             bigint,
    description         varchar(200),
    primary key (checkpoint_id),
    constraint fk_checkpoint_device foreign key (device_id) references deviceInterface(device_id),
    constraint fk_checkpoint_enum foreign key (enum_id) references checkpoint_enum_master(enum_id)
);