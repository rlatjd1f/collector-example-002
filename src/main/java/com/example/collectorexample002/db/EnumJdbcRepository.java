package com.example.collectorexample002.db;

import com.example.collectorexample002.db.mapper.EnumDetailRowMapper;
import com.example.collectorexample002.db.record.CheckpointEnumCode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class EnumJdbcRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public EnumJdbcRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<CheckpointEnumCode> findAllEnumCodes() {
        String sql = "select ed.enum_id, ed.enum_code, ed.enum_value " +
                "from checkpoint_enum_code ed " +
                "left join checkpoint_enum_master em " +
                "on ed.enum_id = em.enum_id";
        return jdbcTemplate.query(sql, new EnumDetailRowMapper());
    }
}
