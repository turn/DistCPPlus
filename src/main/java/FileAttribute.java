

import java.util.EnumSet;

enum FileAttribute
{
	BLOCK_SIZE, REPLICATION, USER, GROUP, PERMISSION, TIMESTAMP;

	final char symbol;

	private FileAttribute() {symbol = toString().toLowerCase().charAt(0);}

	static EnumSet<FileAttribute> parse(String s) {
		if (s == null || s.length() == 0) {
			return EnumSet.allOf(FileAttribute.class);
		}

		EnumSet<FileAttribute> set = EnumSet.noneOf(FileAttribute.class);
		FileAttribute[] attributes = values();

		for(char c : s.toCharArray()) {
			int i = 0;
			for(; i < attributes.length && c != attributes[i].symbol; i++);

			if (i < attributes.length) {
				if (!set.contains(attributes[i])) {
					set.add(attributes[i]);
				} else {

					throw new IllegalArgumentException("There are more than one '"
							+ attributes[i].symbol + "' in " + s);
				}
			} else {
				throw new IllegalArgumentException("'" + c + "' in " + s
						+ " is undefined.");
			}
		}
		return set;
	}
}
