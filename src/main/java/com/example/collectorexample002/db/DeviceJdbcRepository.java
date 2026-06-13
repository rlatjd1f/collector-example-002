package com.example.collectorexample002.db;

import com.example.collectorexample002.db.record.Device;
import com.example.collectorexample002.db.record.ModbusRegister;
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
    private final RowMapper<ModbusRegister> modbusRegisterRowMapper = new DataClassRowMapper<>(ModbusRegister.class);

    public DeviceJdbcRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Device> findAllDevice() {
        String sql = "Select device_id, protocol_id, device_name, device_host, device_port from device";
        return jdbcTemplate.query(sql, deviceRowMapper);
    }

    public List<ModbusRegister> findRegisterByDeviceId(Long deviceId) {
        String sql = "Select device_id       \n" +
                ",register_id     \n" +
                ",register_address\n" +
                ",register_count  \n" +
                ",data_type       \n" +
                ",description     \n" +
                ",polling_cycle from modbus_register where device_id = :device_id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("device_id", deviceId);

        return jdbcTemplate.query(sql, params, modbusRegisterRowMapper);
    }
}
