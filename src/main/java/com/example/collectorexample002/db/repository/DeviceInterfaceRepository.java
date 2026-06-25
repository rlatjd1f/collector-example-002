package com.example.collectorexample002.db.repository;

import com.example.collectorexample002.db.mapper.CheckpointsRowMapper;
import com.example.collectorexample002.db.record.CheckpointModbus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class DeviceInterfaceRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public DeviceInterfaceRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<CheckpointModbus> findCheckpointByInterface(Long interfaceId) {
        String sql = "Select checkpoint_id, checkpoint_address, checkpoint_count, data_type, data_unit, calculate, value_type, enum_id, description \n" +
                " from checkpoint_modbus where interface_id = :interface_id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("interface_id", interfaceId);

        return jdbcTemplate.query(sql, params, new CheckpointsRowMapper());
    }
}
