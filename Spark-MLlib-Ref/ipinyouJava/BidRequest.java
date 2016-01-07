
import java.io.Serializable;

public class BidRequest implements Serializable{

	private static final long serialVersionUID = -3012027079030559912L;

	// private String auction_id; 	              //	cac6f2
	private String create_datetime;	      //	2015-08-20 11-30-30

	private Integer day;	//
	private Integer hr;	//

	private String exchange_id;					// 1,2
	private String app_id ;	      //	app_id
	private String cat;			//
	private String publish_id ;	      //	1
	private Float bidfloor;		//0.0
	private Integer w;		// 320
	private Integer h;		// 50
	private String btype;		// WLAN
	private String	mimes;	//
	private	String os;	//Android
	private String osv;	// 8.0
	private String model;		// Ipad
	private Integer connectiontype;	// 2
	private String country;		//USA
	private String ua;	      // Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0
	private String carrier;	// 310-003
	private Integer js;		// 0
	private String carriername;			//
	private String badv;	//
	private String bcat;	//
	private Integer user;		//1,0


	// public String getAuction_Id(){
	// 	return auction_id;
	// }
	//
	// public void setAuction_Id(String auction_id){
	// 	this.auction_id = auction_id;
	// }

	public String getCreateDateTime() {
		return create_datetime;
		// return day;
		// return hr;
	}

	public void setCreateDateTime(String create_datetime) {
		this.create_datetime = create_datetime;
		// String[] times = create_datetime.split(" ");
		// String[] days = times[0].split("-");
		// String[] hrs = times[1].split(":");
		// Integer day = Integer.valueOf(days[2]) % 7 ;
		// Integer hr = Integer.valueOf(hrs[0]);
	}

	public String getExchangeId(){
		return exchange_id;
	}

	public void setExchangeId(String exchange_id){
		this.exchange_id = exchange_id;
	}

	public String getAppId() {
		return app_id;
	}

	public void setAppId(String app_id) {
		this.app_id = app_id;
	}

	public String getCat() {
		return cat;
	}

	public void setCat(String cat) {
		this.cat = cat;
	}

	public String getPublishIp() {
		return publish_id;
	}

	public void setPublishIp(String publish_id) {
		this.publish_id = publish_id;
	}

	public Float getBidFloor() {
		return bidfloor;
	}

	public void setBidFloor(Float bidfloor) {
		this.bidfloor = bidfloor;
	}

	public Integer getWidth() {
		return w;
	}

	public void setWidth(Integer w) {
		this.w = w;
	}

	public Integer getHeight() {
		return h;
	}

	public void setHeight(Integer h) {
		this.h = h;
	}

	public String getBtype() {
		return btype;
	}

	public void setBtype(String btype) {
		this.btype = btype;
	}

	public String getMimes() {
		return mimes;
	}
	public void setMimes(String mimes) {
		this.mimes = mimes;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getOsv() {
		return osv;
	}
	public void setOsv(String osv) {
		this.osv = osv;
	}

	public String getModel(){
		return model;
	}
	public void setModel(String model){
		this.model=model;
	}

	public Integer getConnectionType() {
		return connectiontype;
	}

	public void setConnectionType(Integer connectiontype) {
		this.connectiontype = connectiontype;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getUa() {
		return ua;
	}

	public void setUa(String ua) {
		this.ua = ua;
	}

	public String getCarrier(){
		return carrier;
	}
	public void setCarrier(String carrier){
		this.carrier = carrier;
	}

	public Integer getJs() {
		return js;
	}

	public void setJs(Integer js) {
		this.js = js;
	}

	public String getCarrierName() {
		return carriername;
	}

	public void setCarrierName(String carriername) {
		this.carriername = carriername;
	}

	public String getBadv() {
		return badv;
	}
	public void setBadv(String badv) {
		this.badv = badv;
	}


	public String getBcat() {
		return bcat;
	}
	public void setBcat(String bcat) {
		this.bcat = bcat;
	}

	public Integer getUser() {
		return user;
	}
	public void setUser(Integer user) {
		this.user = user;
	}
}
