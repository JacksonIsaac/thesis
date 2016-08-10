package cgl.imr.samples.matrix.fox;

import cgl.imr.base.Key;
import cgl.imr.base.ReducerSelector;
import cgl.imr.types.IntKey;

public class IntNoBasedReducerSelector extends ReducerSelector{
	@Override
	public int getReducerNumber(Key key) {
		IntKey k=(IntKey)key;		
		return k.getKey().intValue();
	}

}
