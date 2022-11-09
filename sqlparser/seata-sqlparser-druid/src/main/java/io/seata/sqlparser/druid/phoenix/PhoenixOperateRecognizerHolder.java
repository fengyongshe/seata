package io.seata.sqlparser.druid.phoenix;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.seata.common.loader.LoadLevel;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.druid.SQLOperateRecognizerHolder;
import io.seata.sqlparser.util.JdbcConstants;

@LoadLevel(name = JdbcConstants.PHOENIX)
public class PhoenixOperateRecognizerHolder implements SQLOperateRecognizerHolder  {

  @Override
  public SQLRecognizer getDeleteRecognizer(String sql, SQLStatement ast) {
    return null;
  }

  @Override
  public SQLRecognizer getInsertRecognizer(String sql, SQLStatement ast) {
    return null;
  }

  @Override
  public SQLRecognizer getUpdateRecognizer(String sql, SQLStatement ast) {
    return new PhoenixUpsertRecognizer(sql, ast);
  }

  @Override
  public SQLRecognizer getSelectForUpdateRecognizer(String sql, SQLStatement ast) {
    return null;
  }
}
