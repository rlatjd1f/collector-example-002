package com.example.collectorexample002.db;

import com.example.collectorexample002.db.mapper.CheckpointsRowMapper;
import com.example.collectorexample002.db.mapper.DeviceRowMapper;
import com.example.collectorexample002.db.record.CheckpointModbus;
import com.example.collectorexample002.db.record.DeviceInterface;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
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
        String sql = "select d.device_id ,p.protocol_id ,di.interface_id ,p.protocol_name ,di.unit_id ,d.device_name,di.interface_host ,di.interface_port " +
                "from device_interface di " +
                "left join device d on d.device_id = di.device_id " +
                "left join protocol p on di.protocol_id = p.protocol_id";
        return jdbcTemplate.query(sql, new DeviceRowMapper());
    }

    public List<CheckpointModbus> findCheckpointByDeviceId(Long deviceId) {
        String sql = "Select checkpoint_id, checkpoint_address, checkpoint_count, data_type, data_unit, calculate, value_type, enum_id, description \n" +
                   " from checkpoint_modbus where device_id = :device_id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("device_id", deviceId);

        return jdbcTemplate.query(sql, params, new CheckpointsRowMapper());
    }
}
