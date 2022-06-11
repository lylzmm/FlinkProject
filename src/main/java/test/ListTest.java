package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ListTest {
    public static void main(String[] args) {

        List<Integer> vcs = new ArrayList<>();

        vcs.add(1);
        vcs.add(2);
        vcs.add(0);

        vcs.sort((o1, o2) -> o1.compareTo(o2));

        System.out.println(Arrays.toString(vcs.toArray()));
    }
}
