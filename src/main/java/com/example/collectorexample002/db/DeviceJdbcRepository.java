package com.example.collectorexample002.db;

import com.example.collectorexample002.db.mapper.CheckpointsRowMapper;
import com.example.collectorexample002.db.mapper.DeviceRowMapper;
import com.example.collectorexample002.db.record.Checkpoints;
import com.example.collectorexample002.db.record.Device;
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

    public List<Device> findAllDevice() {
        String sql = "Select d.device_id, p.protocol_name, d.unit_id, d.device_name, d.device_host, d.device_port " +
                "from device d left join protocol p on d.protocol_id = p.protocol_id";
        return jdbcTemplate.query(sql, new DeviceRowMapper());
    }

    public List<Checkpoints> findCheckpointByDeviceId(Long deviceId) {
        String sql = "Select checkpoint_address, checkpoint_count, data_type, data_unit, calculate, value_type, enum_id, description \n" +
                   " from checkpoints where device_id = :device_id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("device_id", deviceId);

        return jdbcTemplate.query(sql, params, new CheckpointsRowMapper());
    }
}
