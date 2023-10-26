// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class DateTimeCommutativeRule extends BottomUpScalarOperatorRewriteRule {

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (!predicate.getChild(1).isConstantRef() || !OperatorType.CALL.equals(predicate.getChild(0).getOpType())) {
            return predicate;
        }

        if (!(predicate.getChild(0).getType().isDatetime() && predicate.getChild(1).getType().isDatetime())) {
            return predicate;
        }

        CallOperator call = (CallOperator) predicate.getChild(0);
        if (!FunctionSet.CONVERT_TZ.equalsIgnoreCase(call.getFnName())) {
            return predicate;
        }

        ScalarOperator s1 = call.getChild(0);
        ConstantOperator sourceTimeZone = (ConstantOperator) call.getChild(1);
        ConstantOperator targetTimeZone = (ConstantOperator) call.getChild(2);
        ConstantOperator result = (ConstantOperator) predicate.getChild(1);
        LocalDateTime equalDateTime = result.getDatetime().atZone(ZoneId.of(targetTimeZone.getVarchar()))
                .withZoneSameInstant(ZoneId.of(sourceTimeZone.getVarchar())).toLocalDateTime();

        return new BinaryPredicateOperator(predicate.getBinaryType(), s1,
                ConstantOperator.createDatetime(equalDateTime, Type.DATETIME));

    }
}
