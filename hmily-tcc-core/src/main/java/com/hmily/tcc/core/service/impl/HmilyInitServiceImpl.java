/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hmily.tcc.core.service.impl;

import com.hmily.tcc.common.config.TccConfig;
import com.hmily.tcc.common.enums.RepositorySupportEnum;
import com.hmily.tcc.common.enums.SerializeEnum;
import com.hmily.tcc.common.serializer.KryoSerializer;
import com.hmily.tcc.common.serializer.ObjectSerializer;
import com.hmily.tcc.common.utils.LogUtil;
import com.hmily.tcc.common.utils.ServiceBootstrap;
import com.hmily.tcc.core.coordinator.CoordinatorService;
import com.hmily.tcc.core.disruptor.publisher.HmilyTransactionEventPublisher;
import com.hmily.tcc.core.helper.SpringBeanUtils;
import com.hmily.tcc.core.service.HmilyInitService;
import com.hmily.tcc.core.spi.CoordinatorRepository;
import com.hmily.tcc.core.spi.repository.JdbcCoordinatorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/**
 * hmily tcc init service.
 *
 * @author xiaoyu
 */
@Service("tccInitService")
public class HmilyInitServiceImpl implements HmilyInitService {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HmilyInitServiceImpl.class);

    private final CoordinatorService coordinatorService;

    private final HmilyTransactionEventPublisher hmilyTransactionEventPublisher;

    @Autowired
    public HmilyInitServiceImpl(final CoordinatorService coordinatorService, final HmilyTransactionEventPublisher hmilyTransactionEventPublisher) {
        this.coordinatorService = coordinatorService;
        this.hmilyTransactionEventPublisher = hmilyTransactionEventPublisher;
    }

    /**
     * hmily initialization.
     *
     * @param tccConfig {@linkplain TccConfig}
     */
    @Override
    public void initialization(final TccConfig tccConfig) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> LOGGER.info("hmily shutdown now")));
        try {
            // 使用SPI机制加载配置
            loadSpiSupport(tccConfig);
            // 协调者 消息生产和处理
            hmilyTransactionEventPublisher.start(tccConfig.getBufferSize(), tccConfig.getConsumerThreads());
            coordinatorService.start(tccConfig);
        } catch (Exception ex) {
            LogUtil.error(LOGGER, " hmily init exception:{}", ex::getMessage);
            System.exit(1);
        }
        LogUtil.info(LOGGER, () -> "hmily init success!");
    }

    /**
     * load spi.
     *
     * @param tccConfig {@linkplain TccConfig}
     */
    private void loadSpiSupport(final TccConfig tccConfig) {
        //spi serialize,获取tccConfig指定的序列化的枚举值
        final SerializeEnum serializeEnum = SerializeEnum.acquire(tccConfig.getSerializer());
        // ServiceLoad.load(clazz) 根据传入的接口类clazz，遍历META-INF/services目录下的以该类命名的文件中的所有类，并实例化返回。
        final ServiceLoader<ObjectSerializer> objectSerializers = ServiceBootstrap.loadAll(ObjectSerializer.class);
        // 从上面获取的子类获取和配置中对应的
        final ObjectSerializer serializer = StreamSupport.stream(objectSerializers.spliterator(), false)
                .filter(objectSerializer -> Objects.equals(objectSerializer.getScheme(), serializeEnum.getSerialize()))
                .findFirst().orElse(new KryoSerializer());
        //spi repository，获取所有事务存储方式的枚举值
        final RepositorySupportEnum repositorySupportEnum = RepositorySupportEnum.acquire(tccConfig.getRepositorySupport());
        final ServiceLoader<CoordinatorRepository> recoverRepositories = ServiceBootstrap.loadAll(CoordinatorRepository.class);
        final CoordinatorRepository repository = StreamSupport.stream(recoverRepositories.spliterator(), false)
                .filter(recoverRepository -> Objects.equals(recoverRepository.getScheme(), repositorySupportEnum.getSupport()))
                .findFirst().orElse(new JdbcCoordinatorRepository());
        repository.setSerializer(serializer);
        // 将事务存储方式存入 Spring上下文
        SpringBeanUtils.getInstance().registerBean(CoordinatorRepository.class.getName(), repository);
    }
}
