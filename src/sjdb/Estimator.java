package sjdb;

import org.w3c.dom.Attr;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {

	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	/*
	 * Consider whether to delete duplicate values or not?
	 * T(proj(atr, R)) = T(R)
	 * 
	 * Project specification outlines assuming no duplicate detection
	 */
	public void visit(Project op) {
		// If I'm correct this is getting the operation input to the projection
		// then getting the output of that relation, which should be a depth
		// traversal of the query tree
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());
		List<Attribute> projectAttrs = op.getAttributes();

		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			Attribute attr = iter.next();
			if (projectAttrs.contains(attr)) {
				output.addAttribute(new Attribute(attr)); 
			}
		}

		op.setOutput(output);
	}
	
	/*
	 * Consider the cases of selcting value or attribute
	 * 
	 * I don't know if range selection is within scope
	 */
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		int outputSize;
		int selectivity;
		// Determine the form of the predicate (attr=attr or attr=value)
		if (op.getPredicate().equalsValue()) {
			// T(σA=c(R)) = T(R)/V(R,A), V(σA=c(R),A) = 1
			// Textbook mentions zipfian distribution of values across an attribute
			// Which may be worth a consideration after everything else is implemented

			// Check for inequality / range comparitor?
			// T(σA>c(R)) = T(R)/3
			// according to textbook is a standard estimator

			// Not equals
			// Either
			// T(σA!=c(R)) = T(R)
			// T(σA!=c(R)) = T(R)(V(R, A) —l ) / V ( R , A)

			// attr=val
			// T(σA=c(R)) = T(R) / V(R,A),
			// V(σA=c(R),A) = 1
			int T_R = input.getTupleCount();
			Attribute A = input.getAttribute(op.getPredicate().getLeftAttribute());
			int V_A = input.getAttribute(A).getValueCount();

			outputSize = T_R/V_A;
			selectivity = 1;

			// Reconstruct the output
			Relation output = new Relation(outputSize);
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute iterAttr = iter.next();
				if(iterAttr == A) output.addAttribute(new Attribute(iterAttr.getName(), selectivity));
				else output.addAttribute(new Attribute(iterAttr));
			}
			op.setOutput(output);

		} else {
			// attr=attr
			// T(σA=B(R)) = T(R)/max(V(R,A),V(R,B)),
			// V(σA=B(R),A) = V(σA=B(R),B) = min(V(R,A), V(R,B)
			int T_R = input.getTupleCount();
			Attribute A = input.getAttribute(op.getPredicate().getLeftAttribute());
			Attribute B = input.getAttribute(op.getPredicate().getRightAttribute());
			int V_A = input.getAttribute(A).getValueCount();
			int V_B = input.getAttribute(B).getValueCount();
			outputSize = T_R/Math.max(V_A, V_B);
			selectivity = Math.min(V_A, V_B);

			// Reconstruct the output
			Relation output = new Relation(outputSize);
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute iterAttr = iter.next();
				if(iterAttr == A || iterAttr == B) output.addAttribute(new Attribute(iterAttr.getName(), selectivity));
				else output.addAttribute(new Attribute(iterAttr));
			}
			op.setOutput(output);
		}
	}
	
	public void visit(Product op) {
		// T(RxS) = T(R)*T(S)
		Relation leftRel = op.getLeft().getOutput();
		Relation rightRel = op.getRight().getOutput();

		int outputSize = (leftRel.getTupleCount() * rightRel.getTupleCount());

		// Construct the new relation
		Relation output = new Relation(outputSize);
		Iterator<Attribute> iterL = leftRel.getAttributes().iterator();
		while (iterL.hasNext()) {
			output.addAttribute(new Attribute(iterL.next()));
		}
		Iterator<Attribute> iterR = rightRel.getAttributes().iterator();
		while (iterR.hasNext()) {
			output.addAttribute(new Attribute(iterR.next()));
		}
		op.setOutput(output);
	}
	
	public void visit(Join op) {
		//T(R⨝A=BS) = T(R)T(S)/max(V(R,A),V(S,B)), V(R⨝A=BS,A) = V(R⨝A=BS,B) = min(V(R,A), V(S,B))
		/*
		(assume that A is an attribute of R and B is an attribute of S)
		Note that, for an attribute C of R that is not a join attribute, V(R⨝A=BS,C) = V(R,C)
		(similarly for an attribute of S that is not a join attribute)
		equijoin with a predicate of the form attr=attr
		No selfjoins

		T(R⨝A=BS) = T(R)T(S)/max(V(R,A),V(S,B))
		V(R⨝(A=B)S,A) = V(R⨝(A=B)S,B) = min(V(R,A), V(S,B))
		 */

		Relation leftRel = op.getLeft().getOutput();
		Relation rightRel = op.getRight().getOutput();

		Attribute A = op.getPredicate().getLeftAttribute();
		Attribute B = op.getPredicate().getRightAttribute();

		int T_A = leftRel.getTupleCount();
		int T_B = rightRel.getTupleCount();
		int V_A = leftRel.getAttribute(A).getValueCount();
		int V_B = rightRel.getAttribute(B).getValueCount();

		int selectivity = Math.min(V_A, V_B);

		int outputSize = (leftRel.getTupleCount() * rightRel.getTupleCount());

		Relation output = new Relation(outputSize);

		Iterator<Attribute> iterL = leftRel.getAttributes().iterator();
		while (iterL.hasNext()) {
			Attribute iterAttr = iterL.next();
			if(iterAttr == A || iterAttr == B) output.addAttribute(new Attribute(iterAttr.getName(), selectivity));
			else output.addAttribute(new Attribute(iterAttr));
		}
		Iterator<Attribute> iterR = rightRel.getAttributes().iterator();
		while (iterR.hasNext()) {
			Attribute iterAttr = iterR.next();
			if(iterAttr == A || iterAttr == B) output.addAttribute(new Attribute(iterAttr.getName(), selectivity));
			else output.addAttribute(new Attribute(iterAttr));
		}

		op.setOutput(output);
	}
}
