package io.seata.rm.datasource.exec.phoenix;

import io.seata.common.loader.LoadLevel;
import io.seata.common.loader.Scope;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.exec.BaseInsertExecutor;
import io.seata.rm.datasource.exec.StatementCallback;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.struct.Defaultable;
import io.seata.sqlparser.struct.Null;
import io.seata.sqlparser.struct.SqlMethodExpr;
import io.seata.sqlparser.struct.SqlSequenceExpr;
import io.seata.sqlparser.util.JdbcConstants;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@LoadLevel(name = JdbcConstants.PHOENIX, scope = Scope.PROTOTYPE)
public class PhoenixUpsertExecutor extends BaseInsertExecutor implements Defaultable {

  /**
   * Instantiates a new Abstract dml base executor.
   *
   * @param statementProxy    the statement proxy
   * @param statementCallback the statement callback
   * @param sqlRecognizer     the sql recognizer
   */
  public PhoenixUpsertExecutor(StatementProxy statementProxy,
                               StatementCallback statementCallback,
                               SQLRecognizer sqlRecognizer) {
    super(statementProxy, statementCallback, sqlRecognizer);
  }

  @Override
  public Map<String, List<Object>> getPkValues() throws SQLException {
    return getPkValuesByColumn();
  }

  @Override
  public Map<String, List<Object>> getPkValuesByColumn() throws SQLException {
    Map<String, List<Object>> pkValuesMap = parsePkValuesFromStatement();
    Set<String> keySet = pkValuesMap.keySet();
    for(String pkKey: keySet) {
      List<Object> pkValues = pkValuesMap.get(pkKey);
      for (int i = 0; i < pkValues.size(); i++) {
        if (!pkKey.isEmpty() && pkValues.get(i) instanceof SqlSequenceExpr) {
          pkValues.set(i, getPkValuesBySequence((SqlSequenceExpr) pkValues.get(i), pkKey).get(0));
        } else if (!pkKey.isEmpty() && pkValues.get(i) instanceof SqlMethodExpr) {
          pkValues.set(i, getGeneratedKeys(pkKey).get(0));
        } else if (!pkKey.isEmpty() && pkValues.get(i) instanceof Null) {
          pkValues.set(i, getGeneratedKeys(pkKey).get(0));
        }
      }
      pkValuesMap.put(pkKey, pkValues);
    }
    return pkValuesMap;
  }

  @Override
  public List<Object> getPkValuesByDefault() throws SQLException {
    return Arrays.asList(getPkValues().values().toArray());
  }

  @Override
  public List<Object> getPkValuesByDefault(String pkKey) throws SQLException {
    return getPkValues().get(pkKey);
  }
}
