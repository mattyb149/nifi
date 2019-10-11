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
package org.apache.nifi.reporting.metrics.event.handlers;

import org.apache.nifi.logging.ComponentLog;
import org.mvel2.MVEL;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.Map;

public class ExpressionHandler extends AbstractEventHandler {

    enum Type{
        MVEL, SPEL;
    }

    public ExpressionHandler(ComponentLog logger) {
        super(logger);
    }

    @Override
    public void execute(Map<String, Object> metrics, Map<String, String> attributes) {
        final String command = attributes.get("command");
        final String type = attributes.get("type");
        final ExpressionParser parser = new SpelExpressionParser();
        try {
            Type expressionType = Type.valueOf(type);
            if(expressionType.equals(Type.MVEL)){
                MVEL.executeExpression( MVEL.compileExpression(command), metrics);
                logger.debug("Expression was executed successfully: {}: {}", new Object[]{type, command});
            }else if(expressionType.equals(Type.SPEL)){
                StandardEvaluationContext context = new StandardEvaluationContext();
                context.setRootObject(metrics);
                context.setVariables(metrics);
                Expression expression = parser.parseExpression(command);
                Object value = expression.getValue(context);
                logger.debug("Expression was executed successfully with result: {}. {}: {}", new Object[]{value,type, command});
            }
        }catch (Exception ex){
            logger.warn("Error occurred when attempting to execute expression. Metrics: {}, Attributes - {}",
                        new Object[]{metrics, attributes}, ex);
        }

    }
}
