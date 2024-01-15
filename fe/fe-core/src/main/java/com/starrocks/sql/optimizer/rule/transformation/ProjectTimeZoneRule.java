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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class ProjectTimeZoneRule extends TransformationRule {

    public ProjectTimeZoneRule() {
        super(RuleType.TIME_ZONE_TRANSFORM, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator logicalOlapScan = (LogicalOlapScanOperator) input.getOp();
        if (logicalOlapScan.hasTransformTz()) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator logicalOlapScan = (LogicalOlapScanOperator) input.getOp();

        Map<ColumnRefOperator, ColumnRefOperator> dateColToNewCol = Maps.newHashMap();
        //build project operator
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        for (ColumnRefOperator col : logicalOlapScan.getColRefToColumnMetaMap().keySet()) {
            if (col.getType().isDateType()) {
                ColumnRefOperator newCol = columnRefFactory.create(col, col.getType(), col.isNullable());
                dateColToNewCol.put(col, newCol);
                projectMap.put(col, convertTZ(context, newCol));
            } else {
                projectMap.put(col, col);
            }
        }

        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectMap);

        //build table scan operator
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = Maps.newHashMap();
        columnMetaToColRefMap.putAll(logicalOlapScan.getColumnMetaToColRefMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
        colRefToColumnMetaMap.putAll(logicalOlapScan.getColRefToColumnMetaMap());
        for (ColumnRefOperator col : dateColToNewCol.keySet()) {
            columnRefFactory.updateColumnRefToColumns(col);
            Column column = logicalOlapScan.getColRefToColumnMetaMap().get(col);
            columnMetaToColRefMap.put(column, dateColToNewCol.get(col));
            colRefToColumnMetaMap.put(dateColToNewCol.get(col), column);
            colRefToColumnMetaMap.remove(col);
        }
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectMap);
        ScalarOperator newPredicate = rewriter.rewrite(logicalOlapScan.getPredicate());

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(logicalOlapScan);
        builder.setColRefToColumnMetaMap(ImmutableMap.copyOf(colRefToColumnMetaMap));
        builder.setColumnMetaToColRefMap(ImmutableMap.copyOf(columnMetaToColRefMap));
        builder.setPredicate(newPredicate);
        builder.setHasTransformTz(true);

        return Lists.newArrayList(OptExpression.create(logicalProjectOperator, new OptExpression(builder.build())));
    }

    private CallOperator convertTZ(OptimizerContext context, ScalarOperator col) {
        // for check timzone is valid
        ZoneId.of(context.getSessionVariable().getSourceTimeZone());
        ZoneId.of(context.getSessionVariable().getTargetTimeZone());

        boolean isDate = col.getType().isDate();
        if (isDate) {
            col = new CastOperator(Type.DATETIME, col);
        }

        Type[] argTypes = new Type[] {Type.DATETIME, Type.VARCHAR, Type.VARCHAR};
        Function func =
                Expr.getBuiltinFunction(FunctionSet.CONVERT_TZ, argTypes, Function.CompareMode.IS_IDENTICAL);
        ConstantOperator sourceTz = ConstantOperator.createVarchar(context.getSessionVariable().getSourceTimeZone());
        ConstantOperator targetTz = ConstantOperator.createVarchar(context.getSessionVariable().getTargetTimeZone());

        CallOperator convertTzOperator =
                new CallOperator(FunctionSet.CONVERT_TZ, Type.DATETIME, Lists.newArrayList(col, sourceTz, targetTz),
                        func);

        if (isDate) {
            convertTzOperator = new CastOperator(Type.DATE, convertTzOperator);
        }
        return convertTzOperator;
    }

}
