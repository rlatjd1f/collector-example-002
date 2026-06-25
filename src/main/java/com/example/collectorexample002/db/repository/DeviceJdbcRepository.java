package com.example.collectorexample002.db.repository;

import com.example.collectorexample002.db.mapper.DeviceRowMapper;
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

    public List<DeviceInterface> findAllDevice() {
        String sql = "select di.device_id ,di.protocol_id ,di.interface_id ,p.protocol_name ,di.unit_id ,d.device_name,di.interface_host ,di.interface_port " +
                "from device_interface di " +
                "left join device d on d.device_id = di.device_id " +
                "left join protocol p on di.protocol_id = p.protocol_id";
        return jdbcTemplate.query(sql, new DeviceRowMapper());
    }
}
