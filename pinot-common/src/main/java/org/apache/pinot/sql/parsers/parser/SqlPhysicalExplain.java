package org.apache.pinot.sql.parsers.parser;

import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlPhysicalExplain extends SqlExplain {
  public SqlPhysicalExplain(SqlParserPos pos, SqlNode explicandum, SqlLiteral detailLevel, SqlLiteral depth,
      SqlLiteral format, int dynamicParameterCount) {
    super(pos, explicandum, detailLevel, depth, format, dynamicParameterCount);
  }
}
