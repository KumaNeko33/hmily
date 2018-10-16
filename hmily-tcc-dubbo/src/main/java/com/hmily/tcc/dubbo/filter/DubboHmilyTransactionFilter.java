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

package com.hmily.tcc.dubbo.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.hmily.tcc.annotation.Tcc;
import com.hmily.tcc.common.bean.context.TccTransactionContext;
import com.hmily.tcc.common.bean.entity.Participant;
import com.hmily.tcc.common.bean.entity.TccInvocation;
import com.hmily.tcc.common.constant.CommonConstant;
import com.hmily.tcc.common.enums.TccActionEnum;
import com.hmily.tcc.common.enums.TccRoleEnum;
import com.hmily.tcc.common.exception.TccRuntimeException;
import com.hmily.tcc.common.utils.GsonUtils;
import com.hmily.tcc.core.concurrent.threadlocal.TransactionContextLocal;
import com.hmily.tcc.core.service.executor.HmilyTransactionExecutor;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * impl dubbo filter.
 *
 * @author xiaoyu
 */
@Activate(group = {Constants.SERVER_KEY, Constants.CONSUMER})
public class DubboHmilyTransactionFilter implements Filter {

    private HmilyTransactionExecutor hmilyTransactionExecutor;

    /**
     * this is init by dubbo spi
     * set hmilyTransactionExecutor.
     *
     * @param hmilyTransactionExecutor {@linkplain HmilyTransactionExecutor }
     */
    public void setHmilyTransactionExecutor(final HmilyTransactionExecutor hmilyTransactionExecutor) {
        this.hmilyTransactionExecutor = hmilyTransactionExecutor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Result invoke(final Invoker<?> invoker, final Invocation invocation) throws RpcException {
        String methodName = invocation.getMethodName();
        Class clazz = invoker.getInterface();
        Class[] args = invocation.getParameterTypes();
        final Object[] arguments = invocation.getArguments();
        converterParamsClass(args, arguments);
        Method method = null;
        Tcc tcc = null;
        try {
            method = clazz.getMethod(methodName, args);
            tcc = method.getAnnotation(Tcc.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        if (Objects.nonNull(tcc)) {
            try {
                // 发起者调用dubbo方法时，这里将是发起者的事务上下文
                final TccTransactionContext tccTransactionContext = TransactionContextLocal.getInstance().get();
                if (Objects.nonNull(tccTransactionContext)) {
                    if (tccTransactionContext.getRole() == TccRoleEnum.LOCAL.getCode()) {
                        tccTransactionContext.setRole(TccRoleEnum.INLINE.getCode());
                    }
                    RpcContext.getContext()
                            .setAttachment(CommonConstant.TCC_TRANSACTION_CONTEXT, GsonUtils.getInstance().toJson(tccTransactionContext));
                }
                // 调用远程dubbo方法，返回结果Result
                final Result result = invoker.invoke(invocation);
                // 如果result 没有异常就保存，发起者dubbo这里的tcc只有接口上的空配置，及confirm和cancel方法是空的，故使用 所调用dubbo方法接口名 作为confirm和cancel方法
                if (!result.hasException()) {
                    final Participant participant = buildParticipant(tccTransactionContext, tcc, method, clazz, arguments, args);
                    if (tccTransactionContext.getRole() == TccRoleEnum.INLINE.getCode()) {
                        hmilyTransactionExecutor.registerByNested(tccTransactionContext.getTransId(),
                                participant);
                    } else {
                        // add participant. dubbo方法调用成功，将所调用的方法的 comfirm和cancel方法调用点 添加到 发起者的当前事务中，并同时更新数据库事务的调用点
                        hmilyTransactionExecutor.enlistParticipant(participant);
                    }
                } else {
                    // 远程调用有异常就在dubbo Filter中抛出，然后在 发起者的切面中的拦截后 进行回滚
                    throw new TccRuntimeException("rpc invoke exception{}", result.getException());
                }
                return result;
            } catch (RpcException e) {
                e.printStackTrace();
                throw e;
            }
        } else {
            return invoker.invoke(invocation);
        }
    }

    @SuppressWarnings("unchecked")
    private Participant buildParticipant(final TccTransactionContext tccTransactionContext,
                                         final Tcc tcc,
                                         final Method method, final Class clazz,
                                         final Object[] arguments, final Class... args) throws TccRuntimeException {

        if (Objects.isNull(tccTransactionContext)
                // 发起者刚创建tccTransactionContext的行为状态就是TccActionEnum.TRYING
                || (TccActionEnum.TRYING.getCode() != tccTransactionContext.getAction())) {
            return null;
        }
        //获取协调方法
        String confirmMethodName = tcc.confirmMethod();
        if (StringUtils.isBlank(confirmMethodName)) {
            confirmMethodName = method.getName();
        }
        String cancelMethodName = tcc.cancelMethod();
        if (StringUtils.isBlank(cancelMethodName)) {
            cancelMethodName = method.getName();
        }
        TccInvocation confirmInvocation = new TccInvocation(clazz, confirmMethodName, args, arguments);
        TccInvocation cancelInvocation = new TccInvocation(clazz, cancelMethodName, args, arguments);
        //封装调用点
        return new Participant(tccTransactionContext.getTransId(), confirmInvocation, cancelInvocation);
    }

    private void converterParamsClass(final Class[] args, final Object[] arguments) {
        if (arguments == null || arguments.length < 1) {
            return;
        }
        for (int i = 0; i < arguments.length; i++) {
            args[i] = arguments[i].getClass();
        }
    }
}
