import java.util.HashSet;
import java.util.HashMap;

public class dubtest {
    public static void main(String[] args) {
        HashSet<Double> x = new HashSet<Double>();
        Double a = new Double(2.5);
        x.add(a);
        System.out.println(x.contains(2.5));
        Double b = new Double(2.5);
        System.out.println(x.contains(b));
        System.out.println(1 / 100);
        System.out.println((int) 1.99);
        Long d = new Long(23);
        x.add((double) d.longValue());
        double e = 2.01;
        System.out.println(e - (int) e);
        HashMap<Double, Long> map = new HashMap<Double, Long>();
        map.put(b, d);
        System.out.println(map.get(2.5));
    }
}

