package com.example.collectorexample002.db;

import com.example.collectorexample002.db.record.Device;
import com.example.collectorexample002.db.record.CheckpointMaster;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class DeviceJdbcRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    // 생성자 기반 매핑을 위한 RowMapper 정의
    private final RowMapper<Device> deviceRowMapper = new DataClassRowMapper<>(Device.class);
    private final RowMapper<CheckpointMaster> modbusRegisterRowMapper = new DataClassRowMapper<>(CheckpointMaster.class);

    public DeviceJdbcRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Device> findAllDevice() {
        String sql = "Select device_id, protocol_id, unit_id, device_name, device_host, device_port from device";
        return jdbcTemplate.query(sql, deviceRowMapper);
    }

    public List<CheckpointMaster> findRegisterByDeviceId(Long deviceId) {
        String sql = "Select device_id, checkpoint_id, checkpoint_address, checkpoint_count, data_type, data_unit, calculate, value_type, enum_id, description \n" +
                " from checkpoint_master where device_id = :device_id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("device_id", deviceId);

        return jdbcTemplate.query(sql, params, modbusRegisterRowMapper);
    }
}
