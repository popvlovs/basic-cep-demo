package com.hansight.udfs;

import com.hansight.util.ExpressionUtil;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class BelongFunction extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        ExpressionUtil.belong("None", "None");
    }

    public Boolean eval(String data, String dataSet) {
        return ExpressionUtil.belong(data, dataSet);
    }
}
