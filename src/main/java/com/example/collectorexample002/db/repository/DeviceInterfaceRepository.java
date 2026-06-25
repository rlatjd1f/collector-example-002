package com.example.collectorexample002.db.repository;

import com.example.collectorexample002.db.mapper.CheckpointRowMapper;
import com.example.collectorexample002.db.record.Checkpoint;
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

    public List<Checkpoint> findCheckpointByInterface(Long interfaceId) {
        String sql = "Select id, request_address, request_count, data_type, data_unit, expression, value_type, enum_id, description \n" +
                " from checkpoint where interface_id = :interface_id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("interface_id", interfaceId);

        return jdbcTemplate.query(sql, params, new CheckpointRowMapper());
    }
}
