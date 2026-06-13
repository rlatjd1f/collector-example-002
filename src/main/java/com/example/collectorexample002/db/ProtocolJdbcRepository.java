package com.example.collectorexample002.db;

import com.example.collectorexample002.db.record.Protocol;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ProtocolJdbcRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    private final RowMapper<Protocol> protocolRowMapper = new DataClassRowMapper<>(Protocol.class);

    public ProtocolJdbcRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Protocol> findAllProtocol() {
        String sql = "select protocol_id, protocol_name from protocol";
        return jdbcTemplate.query(sql, protocolRowMapper);
    }
}
