create table protocol (
    protocol_id     bigint auto_increment,
    protocol_name   varchar(100) not null,
    primary key (protocol_id)
);

create table device
(
    device_id       bigint auto_increment,
    protocol_id     bigint not null,
    unit_id         tinyint unsigned not null,
    device_name     varchar(100) not null,
    device_host     varchar(15) not null,
    device_port     smallint unsigned not null,
    primary key (device_id),
    constraint fk_device_protocol foreign key (protocol_id) references protocol (protocol_id)
);

create table enum_master (
    enum_id     bigint auto_increment not null,
    enum_name   varchar(100) not null,
    primary key (enum_id)
);

create table enum_detail (
    enum_id     bigint not null,
    enum_code   int not null,
    enum_name   varchar(100),
    primary key (enum_id, enum_code),
    constraint fk_enum_detail_master foreign key (enum_id) references enum_master(enum_id) on delete cascade
);

create table checkpoint_master (
    device_id           bigint,
    checkpoint_id         bigint auto_increment not null,
    checkpoint_address    int unsigned not null,
    checkpoint_count      int unsigned not null,
    data_type           varchar(20) not null,
    data_unit           varchar(20),
    calculate           varchar(10) not null, -- 성능 계산식
    value_type          varchar(20) not null, -- enumeration 연관관계
    enum_id             bigint,
    description         varchar(200),
    primary key (checkpoint_id),
    constraint fk_checkpoint_device foreign key (device_id) references device(device_id),
    constraint fk_checkpoint_enum foreign key (enum_id) references enum_master(enum_id)
);