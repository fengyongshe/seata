package io.seata.sqlparser.druid.phoenix;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.phoenix.visitor.PhoenixOutputVisitor;
import io.seata.common.util.StringUtils;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.druid.BaseRecognizer;
import io.seata.sqlparser.struct.Null;
import io.seata.sqlparser.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class BasePhoenixRecognizer extends BaseRecognizer  {
  /**
   * Instantiates a new Base recognizer.
   *
   * @param originalSQL the original sql
   */
  public BasePhoenixRecognizer(String originalSQL) {
    super(originalSQL);
  }

  public PhoenixOutputVisitor createOutputVisitor(final ParametersHolder parametersHolder,
                                                  final ArrayList<List<Object>> paramAppenderList,
                                                  final StringBuilder sb) {
    return new PhoenixOutputVisitor(sb) {

      @Override
      public boolean visit(SQLVariantRefExpr x) {
        if ("?".equals(x.getName())) {
          ArrayList<Object> oneParamValues = parametersHolder.getParameters().get(x.getIndex() + 1);
          if (paramAppenderList.isEmpty()) {
            oneParamValues.forEach(t -> paramAppenderList.add(new ArrayList<>()));
          }
          for (int i = 0; i < oneParamValues.size(); i++) {
            Object o = oneParamValues.get(i);
            paramAppenderList.get(i).add(o instanceof Null ? null : o);
          }
        }
        return super.visit(x);
      }
    };
  }

  public String getWhereCondition(SQLExpr where, final ParametersHolder parametersHolder,
                                  final ArrayList<List<Object>> paramAppenderList) {
    if (Objects.isNull(where)) {
      return StringUtils.EMPTY;
    }
    StringBuilder sb = new StringBuilder();
    executeVisit(where, createOutputVisitor(parametersHolder, paramAppenderList, sb));
    return sb.toString();
  }

  public String getWhereCondition(SQLExpr where) {
    if (Objects.isNull(where)) {
      return StringUtils.EMPTY;
    }

    StringBuilder sb = new StringBuilder();

    executeVisit(where, new PhoenixOutputVisitor(sb));
    return sb.toString();
  }

  protected String getLimitCondition(SQLLimit sqlLimit) {
    if (Objects.isNull(sqlLimit)) {
      return StringUtils.EMPTY;
    }

    StringBuilder sb = new StringBuilder();
    executeLimit(sqlLimit, new PhoenixOutputVisitor(sb));

    return sb.toString();
  }

  protected String getOrderByCondition(SQLOrderBy sqlOrderBy) {
    if (Objects.isNull(sqlOrderBy)) {
      return StringUtils.EMPTY;
    }

    StringBuilder sb = new StringBuilder();
    executeOrderBy(sqlOrderBy, new PhoenixOutputVisitor(sb));

    return sb.toString();
  }

  protected String getOrderByCondition(SQLOrderBy sqlOrderBy, final ParametersHolder parametersHolder,
                                       final ArrayList<List<Object>> paramAppenderList) {
    if (Objects.isNull(sqlOrderBy)) {
      return StringUtils.EMPTY;
    }

    StringBuilder sb = new StringBuilder();
    executeOrderBy(sqlOrderBy, createOutputVisitor(parametersHolder, paramAppenderList, sb));
    return sb.toString();
  }

  public String getDbType() {
    return JdbcConstants.PHOENIX;
  }


}
