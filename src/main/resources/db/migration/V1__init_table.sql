create table protocol (
    id      bigint auto_increment,
    name    varchar(100) not null,
    primary key (id)
);

create table device
(
    id      bigint auto_increment,
    name    varchar(100) not null,
    primary key (id)
);

create table device_interface (
    device_id       bigint not null,
    protocol_id     bigint not null,
    id              bigint auto_increment,
    unit_id         tinyint unsigned not null,
    interface_host  varchar(15) not null,
    interface_port  smallint unsigned not null,
    primary key (id),
    constraint fk_device_interface foreign key (device_id) references device (id),
    constraint fk_protocol_interface foreign key (protocol_id) references protocol (id)
);

create table checkpoint_enum_master (
    id          bigint auto_increment not null,
    name   varchar(100) not null,
    primary key (id)
);

create table checkpoint_enum_code (
    enum_id     bigint not null,
    enum_code   int not null,
    enum_value  varchar(100),
    primary key (enum_id, enum_code),
    constraint fk_enum_detail_master foreign key (enum_id) references checkpoint_enum_master(id) on delete cascade
);

create table checkpoint (
    interface_id        bigint,
    id                  bigint auto_increment not null,
    request_address     int unsigned not null,
    request_count       int unsigned not null,
    data_type           varchar(20) not null,
    data_unit           varchar(20),
    expression          varchar(10) not null,
    value_type          varchar(20) not null,
    enum_id             bigint,
    description         varchar(200),
    primary key (id),
    constraint fk_checkpoint_interface foreign key (interface_id) references device_interface(id) on delete cascade,
    constraint fk_checkpoint_enum foreign key (enum_id) references checkpoint_enum_master(id)
);