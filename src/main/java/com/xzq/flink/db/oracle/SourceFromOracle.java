package com.xzq.flink.db.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SourceFromOracle extends RichSourceFunction<CapitalAccount> {

	private static final long serialVersionUID = -8836171930682881569L;

	PreparedStatement ps;
	private Connection connection;

	private static final String QUERY_KFTACCOUNTS = "select CAPITAL_ACCOUNT_ID, USER_ACCOUNT_ID, BALANCE, USEABLE_AMT, "
			+ "DRAWABLE_AMT, FORCE_FREEZE_AMT, IS_OVERDRAW, DIGEST, CREATED_TIME from kftbank2.t_capital_account a "
			+ "where a.capital_account_id in (821066,821065,926006,925956)";

	/**
	 * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		connection = ConnectionUtil.getConnection();
		ps = connection.prepareStatement(QUERY_KFTACCOUNTS);
	}

	/**
	 * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
	 */
	@Override
	public void close() throws Exception {
		super.close();
		if (ps != null) {
			ps.close();
		}
		if (connection != null) {
			connection.close();
		}
	}

	@Override
	public void run(SourceContext<CapitalAccount> ctx) throws Exception {
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			CapitalAccount capitalAccount = new CapitalAccount();
			capitalAccount.setCapitalAccountId(Long.valueOf(rs
					.getLong("CAPITAL_ACCOUNT_ID")));
			capitalAccount
					.setUserAccountId(rs.getString("USER_ACCOUNT_ID"));
			capitalAccount.setBalance(rs.getDouble("BALANCE"));
			capitalAccount.setUseableAmt(rs.getBigDecimal("USEABLE_AMT"));
			capitalAccount.setDrawableAmt(rs.getBigDecimal("DRAWABLE_AMT"));
			capitalAccount.setForceFreezeAmt(rs
					.getBigDecimal("FORCE_FREEZE_AMT"));
			capitalAccount.setDigest(rs.getString("DIGEST"));
			ctx.collect(capitalAccount);
		}

	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

}
