package com.example.collectorexample002.db.repository;

import com.example.collectorexample002.db.mapper.DeviceInterfaceRowMapper;
import com.example.collectorexample002.db.record.DeviceInterface;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class DeviceJdbcRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public DeviceJdbcRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<DeviceInterface> findAllDeviceInterface() {
        String sql = "select di.device_id ,di.protocol_id ,di.id as interface_id ,p.name as protocol_name ,di.unit_id ,d.name as device_name, di.interface_host, di.interface_port " +
                "from device_interface di " +
                "join device d on d.id = di.device_id " +
                "join protocol p on p.id = di.protocol_id";
        return jdbcTemplate.query(sql, new DeviceInterfaceRowMapper());
    }
}
