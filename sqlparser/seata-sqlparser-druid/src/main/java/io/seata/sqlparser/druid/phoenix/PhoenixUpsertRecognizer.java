package io.seata.sqlparser.druid.phoenix;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.phoenix.ast.PhoenixUpsertStatement;
import com.alibaba.druid.sql.dialect.phoenix.visitor.PhoenixOutputVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.seata.common.util.CollectionUtils;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.SQLInsertRecognizer;
import io.seata.sqlparser.SQLType;
import io.seata.sqlparser.SQLUpdateRecognizer;
import io.seata.sqlparser.struct.NotPlaceholderExpr;
import io.seata.sqlparser.struct.Null;
import io.seata.sqlparser.struct.SqlMethodExpr;
import io.seata.sqlparser.util.ColumnUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PhoenixUpsertRecognizer extends BasePhoenixRecognizer implements SQLInsertRecognizer {

  private SQLInsertStatement ast;

  /**
   * Instantiates a new Base recognizer.
   *
   * @param originalSQL the original sql
   */
  public PhoenixUpsertRecognizer(String originalSQL, SQLStatement ast ) {
    super(originalSQL);
    this.ast = (SQLInsertStatement) ast;
  }

  @Override
  public SQLType getSQLType() {
    return SQLType.INSERT;
  }

  @Override
  public String getTableAlias() {
    return ast.getTableSource().getAlias();
  }

  @Override
  public String getTableName() {
    StringBuilder sb = new StringBuilder();
    PhoenixOutputVisitor visitor = new PhoenixOutputVisitor(sb) {
      @Override
      public boolean visit(SQLExprTableSource x) {
        printTableSourceExpr(x.getExpr());
        return false;
      }
    };
    visitor.visit(ast.getTableSource());
    return sb.toString();
  }

  @Override
  protected SQLStatement getAst() {
    return ast;
  }

  @Override
  public boolean insertColumnsIsEmpty() {
    return CollectionUtils.isEmpty(ast.getColumns());
  }

  @Override
  public List<String> getInsertColumns() {
    List<SQLExpr> columnSQLExprs = ast.getColumns();
    if (columnSQLExprs.isEmpty()) {
      return null;
    }
    List<String> list = new ArrayList<>(columnSQLExprs.size());
    for (SQLExpr expr : columnSQLExprs) {
      if (expr instanceof SQLIdentifierExpr) {
        list.add(((SQLIdentifierExpr)expr).getName());
      } else {
        wrapSQLParsingException(expr);
      }
    }
    return list;
  }

  @Override
  public List<List<Object>> getInsertRows(Collection<Integer> primaryKeyIndex) {
    List<SQLInsertStatement.ValuesClause> valuesClauses = ast.getValuesList();
    List<List<Object>> rows = new ArrayList<>(valuesClauses.size());
    for (SQLInsertStatement.ValuesClause valuesClause : valuesClauses) {
      List<SQLExpr> exprs = valuesClause.getValues();
      List<Object> row = new ArrayList<>(exprs.size());
      rows.add(row);
      for (int i = 0, len = exprs.size(); i < len; i++) {
        SQLExpr expr = exprs.get(i);
        if (expr instanceof SQLNullExpr) {
          row.add(Null.get());
        } else if (expr instanceof SQLValuableExpr) {
          row.add(((SQLValuableExpr) expr).getValue());
        } else if (expr instanceof SQLVariantRefExpr) {
          row.add(((SQLVariantRefExpr) expr).getName());
        } else if (expr instanceof SQLMethodInvokeExpr) {
          row.add(SqlMethodExpr.get());
        } else {
          if (primaryKeyIndex.contains(i)) {
            wrapSQLParsingException(expr);
          }
          row.add(NotPlaceholderExpr.get());
        }
      }
    }
    return rows;
  }

  @Override
  public List<String> getInsertParamsValue() {
    List<SQLInsertStatement.ValuesClause> valuesList = ast.getValuesList();
    List<String> list = new ArrayList<>();
    for (SQLInsertStatement.ValuesClause m: valuesList) {
      String values = m.toString().replace("VALUES", "").trim();
      // when all params is constant, the length of values less than 1
      if (values.length() > 1) {
        values = values.substring(1,values.length() - 1);
      }
      list.add(values);
    }
    return list;
  }

  @Override
  public List<String> getDuplicateKeyUpdate() {
    return null;
  }

  @Override
  public List<String> getInsertColumnsIsSimplified() {
    List<String> insertColumns = getInsertColumns();
    return ColumnUtils.delEscape(insertColumns, getDbType());
  }
}
