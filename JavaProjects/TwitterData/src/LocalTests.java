import java.io.FileNotFoundException;

import sg.edu.nus.cs5344.spring14.twitter.CmdArguments;



public class LocalTests {

	public static void main(String[] args) throws FileNotFoundException {

		CmdArguments.instantiate(new String[]{"-d","/datalol"});
		System.out.println(CmdArguments.getArgs().getWorkingDir());

	}
}
