package com.xzq.flink.db.oracle;

import java.math.BigDecimal;
import java.util.Date;

public class CapitalAccount {
    private Long capitalAccountId;

    private String userAccountId;

    private Double balance;

    private BigDecimal useableAmt;

    private BigDecimal drawableAmt;

    private BigDecimal forceFreezeAmt;

    private Integer isOverdraw;

    private String digest;

    private Date createdTime;

    public Long getCapitalAccountId() {
        return capitalAccountId;
    }

    public void setCapitalAccountId(Long capitalAccountId) {
        this.capitalAccountId = capitalAccountId;
    }

    public String getUserAccountId() {
        return userAccountId;
    }

    public void setUserAccountId(String userAccountId) {
        this.userAccountId = userAccountId == null ? null : userAccountId.trim();
    }

    public Double getBalance() {
        return balance;
    }

    public void setBalance(Double balance) {
        this.balance = balance;
    }

    public BigDecimal getUseableAmt() {
        return useableAmt;
    }

    public void setUseableAmt(BigDecimal useableAmt) {
        this.useableAmt = useableAmt;
    }

    public BigDecimal getDrawableAmt() {
        return drawableAmt;
    }

    public void setDrawableAmt(BigDecimal drawableAmt) {
        this.drawableAmt = drawableAmt;
    }

    public BigDecimal getForceFreezeAmt() {
        return forceFreezeAmt;
    }

    public void setForceFreezeAmt(BigDecimal forceFreezeAmt) {
        this.forceFreezeAmt = forceFreezeAmt;
    }

    public Integer getIsOverdraw() {
        return isOverdraw;
    }

    public void setIsOverdraw(Integer isOverdraw) {
        this.isOverdraw = isOverdraw;
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest == null ? null : digest.trim();
    }

    public Date getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
    }

	@Override
	public String toString() {
		return "CapitalAccount [capitalAccountId=" + capitalAccountId
				+ ", userAccountId=" + userAccountId + ", balance=" + balance
				+ ", useableAmt=" + useableAmt + ", drawableAmt=" + drawableAmt
				+ ", forceFreezeAmt=" + forceFreezeAmt + ", isOverdraw="
				+ isOverdraw + ", digest=" + digest + ", createdTime="
				+ createdTime + "]";
	}
    
}