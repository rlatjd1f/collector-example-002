package com.example.collectorexample002.db.mapper;

import com.example.collectorexample002.db.record.Checkpoints;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CheckpointsRowMapper implements RowMapper<Checkpoints> {
    @Override
    public Checkpoints mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new Checkpoints(
                rs.getInt("checkpoint_address"),
                rs.getInt("checkpoint_count"),
                rs.getString("data_type"),
                rs.getString("data_unit"),
                rs.getString("calculate"),
                rs.getString("value_type"),
                rs.getLong("enum_id"),
                rs.getString("description")
        );
    }
}
