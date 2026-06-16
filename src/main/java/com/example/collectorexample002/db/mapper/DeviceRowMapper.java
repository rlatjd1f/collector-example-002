package com.example.collectorexample002.db.mapper;

import com.example.collectorexample002.db.record.Device;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DeviceRowMapper implements RowMapper<Device> {
    @Override
    public Device mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new Device(
                rs.getLong("device_id"),
                rs.getString("protocol_name"),
                rs.getInt("unit_id"),
                rs.getString("device_name"),
                rs.getString("device_host"),
                rs.getInt("device_port")
        );
    }
}
