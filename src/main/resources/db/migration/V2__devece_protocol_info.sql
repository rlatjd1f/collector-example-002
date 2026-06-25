insert into protocol (id, name) values
  (1, "MODBUS"),
  (2, "SNMP");

insert into device (id, name) values
    (1, "PM8240");

insert into device_interface (device_id, protocol_id, id, unit_id, interface_host, interface_port) VALUES
    (1,1,1,1,"192.168.1.53", "502");