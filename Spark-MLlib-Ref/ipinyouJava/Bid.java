
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Bid implements Algorithm{

	/**
	 * Algorithm parameters definition
	 */
	// ratio of bidding in percent
	private int bidRatio = 50;
	// fixed bid price
	private int fixedBidPrice = 300;

	// Config parameters def file
	private String configFile = "algo.conf";
	// private Map<String,String> model = new HashMap<String,String>();

	/**
	 * init() method initializes algorithm parameters and model
	 * Memory limit: 512M
	 * @throws Exception
	 */

	public void init() throws Exception{
		readConfigPara(this.configFile);
	}

	/**
	 * getBidPrice() method makes the real calculation Note: one bid request
	 * decision has to be make less than 20ms.
	 *
	 */

	public int getBidPrice(BidRequest bidRequest){

		int bidPrice = -1;
		Random r = new Random();

		if(r.nextInt(100) < this.bidRatio){
			bidPrice = this.fixedBidPrice;
		}
		return bidPrice;
	}

	/**
	 * Reading config parameters from config file
	 * The config file is defined in key = value format
	 * Lines starting with '#' are regarded as remarks
	 *
	 */


	private void readConfigPara(String filename) throws Exception {
		// TODO Auto-generated method stub
		InputStream inputStream = read(filename);
		String line = null;
		Map<String, String> kvMap = new HashMap<String, String>();

		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

		while((line = br.readLine()) != null){
			if(line.matches("^#.*")){ // skip remark lines starting with #
				continue;
			}
			String[] arr = line.trim().split("=");
			if(arr.length == 2){
				kvMap.put(arr[0], arr[1]);
			}
		}

		if(kvMap.containsKey("bidRatio")){
			this.bidRatio = Integer.parseInt(kvMap.get("bidRatio"));
		}
		if(kvMap.containsKey("fixedBidPrice")){
			this.fixedBidPrice = Integer.parseInt(kvMap.get("fixedBidPrice"));
		}

	}

	/**
	 * read file(from jar files or not ) to inputstream
	 */

	private InputStream read(String filename) {
		// TODO Auto-generated method stub
		InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);

		if(resourceAsStream == null){
			throw new RuntimeException("read file error");
		}
		return resourceAsStream;
	}
}
