AfterMatchSkipStrategy skipStrategy_{uid} = AfterMatchSkipStrategy.skipPastLastEvent();
Pattern<ObjectNode, ObjectNode> pattern_{uid} = Pattern.<ObjectNode>
        begin("pattern_{uid}_A", skipStrategy_{uid}).where(new IterativeCondition<ObjectNode>() {
                @Override
                public boolean filter(ObjectNode val, Context<ObjectNode> context) throws Exception {
                    return {conditionA};
                }
        })
        .followedBy("pattern_{uid}_B").where(new IterativeCondition<ObjectNode>() {
                @Override
                public boolean filter(ObjectNode val, Context<ObjectNode> context) throws Exception {
                    return {conditionB};
                }
        }){within};

DataStream<ObjectNode> input_{uid} = {inputStream};
SingleOutputStreamOperator output_{uid} = CEP.pattern(input_{uid}, pattern_{uid})
        .select(matchedEvents -> {
            {measure}
        })
        .uid("{uid}")