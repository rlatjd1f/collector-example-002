package com.example.collectorexample002.db.mapper;

import com.example.collectorexample002.db.record.EnumDetail;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EnumDetailRowMapper implements RowMapper<EnumDetail> {
    @Override
    public EnumDetail mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new EnumDetail(
                rs.getLong("enum_id"),
                rs.getInt("enum_code"),
                rs.getString("enum_detail_name")
        );
    }
}
