package c.a.mock;

import java.util.Random;

/**
 * @author ???
 * 2019-04-28 12:50
 */
public class RandomNum {

    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

