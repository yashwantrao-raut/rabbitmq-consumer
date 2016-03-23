package direct.data;
public class Offer {

    private   String voucherCode;
    private String desc;
    private   String merchantName;
    private   String terms;
    private String title;

    public Offer() {
    }

    public Offer(String voucherCode, String desc, String merchantName, String terms, String title) {
        this.voucherCode = voucherCode;
        this.desc = desc;
        this.merchantName = merchantName;
        this.terms = terms;
        this.title = title;
    }

    @Override
    public String toString() {
        return " we have received Offer{" +
                "voucherCode='" + voucherCode + '\'' +
                ", desc='" + desc + '\'' +
                ", merchantName='" + merchantName + '\'' +
                ", terms='" + terms + '\'' +
                ", title='" + title + '\'' +
                '}';
    }

    public String getVoucherCode() {
        return voucherCode;
    }

    public void setVoucherCode(String voucherCode) {
        this.voucherCode = voucherCode;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getMerchantName() {
        return merchantName;
    }

    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    public String getTerms() {
        return terms;
    }

    public void setTerms(String terms) {
        this.terms = terms;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


}
