insert into protocol (protocol_id, protocol_name) values (1, "MODBUS");

insert into device (device_id, protocol_id, device_name, device_host, device_port) values (1, 1, "PM8240", "127.0.0.1", "502");
-- insert into device (device_id, protocol_id, device_name, device_host, device_port) values (1, 1, "PM8240", "192.168.1.53", "502");