package org.liu.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.liu.common.util.IkUtil;

public class SentenceSplit implements UDF1<String, String> {
    @Override
    public String call(String sentence) throws Exception {
        StringBuilder builder = new StringBuilder();
        IkUtil.splitSentence(sentence).forEach(word -> builder.append(",").append(word));
        return builder.substring(1);
    }
}
