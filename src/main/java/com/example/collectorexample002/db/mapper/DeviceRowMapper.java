package com.example.collectorexample002.db.mapper;

import com.example.collectorexample002.db.record.DeviceInterface;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DeviceRowMapper implements RowMapper<DeviceInterface> {
    @Override
    public DeviceInterface mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new DeviceInterface(
                rs.getLong("device_id"),
                rs.getLong("protocol_id"),
                rs.getLong("interface_id"),
                rs.getString("protocol_name"),
                rs.getInt("unit_id"),
                rs.getString("device_name"),
                rs.getString("interface_host"),
                rs.getInt("interface_port")
        );
    }
}
