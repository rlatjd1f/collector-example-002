package com.example.collectorexample002.db.mapper;

import com.example.collectorexample002.db.record.Checkpoint;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CheckpointRowMapper implements RowMapper<Checkpoint> {
    @Override
    public Checkpoint mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new Checkpoint(
                rs.getLong("checkpoint_id"),
                rs.getInt("request_address"),
                rs.getInt("request_count"),
                rs.getString("data_type"),
                rs.getString("data_unit"),
                rs.getString("expression"),
                rs.getString("value_type"),
                rs.getLong("enum_id"),
                rs.getString("description")
        );
    }
}
