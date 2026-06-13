package com.example.collectorexample002.netty.pipeline;

import com.example.collectorexample002.modbus.record.ModbusMbapHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ModbusHeaderDecoder extends ByteToMessageDecoder {

    private final static int HEADER_LENGTH = 6;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        // 읽기가능한 바이트 수가 헤더 길이인 6바이트 보다 작으면 대기
        if (in.readableBytes() < HEADER_LENGTH) {
            return;
        }

        // read 인덱스 옮김 없이 앞으로 올 length 만큼의 데이터를 확인하기 위해 마킹
        in.markReaderIndex();

        int transactionId = in.readUnsignedShort(); // 트랜잭션 id 2 byte
        int protocolId = in.readUnsignedShort();    // 프로토콜 id 2 byte
        int length = in.readUnsignedShort();        // pdu 길이 2 byte

        if (in.readableBytes() < length) {
            // markReaderIndex 한 위치로 인덱스 리셋하여 length 만큼 읽어올때까지 리턴
            in.resetReaderIndex();
            return;
        }

        // 헤더 record 생성
        ModbusMbapHeader header = new ModbusMbapHeader(transactionId, protocolId, length);

        // 헤더뒤에 body 데이터를 unit ID + PDU 길이만큼 자름
        ByteBuf payload = in.readRetainedSlice(length);

        out.add(header);
        out.add(payload);
    }
}
