package com.example.collectorexample002.db.mapper;

import com.example.collectorexample002.db.record.CheckpointEnumCode;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CheckpointEnumCodeRowMapper implements RowMapper<CheckpointEnumCode> {
    @Override
    public CheckpointEnumCode mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new CheckpointEnumCode(
                rs.getLong("enum_id"),
                rs.getInt("enum_code"),
                rs.getString("enum_value")
        );
    }
}
