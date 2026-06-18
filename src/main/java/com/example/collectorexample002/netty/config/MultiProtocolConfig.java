package com.example.collectorexample002.netty.config;

import com.example.collectorexample002.netty.pipeline.inbound.ModbusPacketDecoder;
import com.example.collectorexample002.netty.pipeline.inbound.RedisInboundHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class MultiProtocolConfig {

    private final RedisTemplate<String, Object> redisTemplate;

    @Bean(destroyMethod = "shutdownGracefully")
    public EventLoopGroup sharedWorkerGroup() {
        return new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    }

    // 멀티 프로토콜 처리를 위한 Bootstrap Bean 등록
    @Bean
    public Bootstrap modbusBootstrap(EventLoopGroup group) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(
                                260, 4,2,0,0));
                        ch.pipeline().addLast("ModbusPacketDecoder", new ModbusPacketDecoder());
                        ch.pipeline().addLast("RedisInboundHandler", new RedisInboundHandler(redisTemplate));
                    }
                });

        return bootstrap;
    }

    @Bean
    public Bootstrap snmpBootstrap(EventLoopGroup group) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("SnmpFrameDecoder", new LengthFieldBasedFrameDecoder(
                                260, 4,2,0,0));
                        ch.pipeline().addLast("ModbusPacketDecoder", new ModbusPacketDecoder());
                    }
                });

        return bootstrap;
    }


    @Bean
    public Timer hashedWheelTimer() {
        return new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 512);
    }
}
