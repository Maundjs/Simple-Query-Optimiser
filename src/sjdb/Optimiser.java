package sjdb;

import org.w3c.dom.Attr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class Optimiser {

    public Optimiser(Catalogue catalogue) {
        //System.out.println(catalogue);
    }

    public Operator optimise(Operator plan) {
        // Create a query tree
        Node queryTree = constructQueryTree(plan);
        // Attempt to optimise query
        // I should use a heuristical approach - but for now I'll just push selects down before products
        // may also need to include a step to combine cross-product & selections into the equivalent equijoin
        Node optimisedTree = optimiseQuery(queryTree);
        // reconstruct the query
        Operator optimisedQuery = reconstructQuery(optimisedTree);
        return optimisedQuery;
    }

    // Reorder so that selects happen after the relevant scan or product
    private Node optimiseQuery(Node tree) {
        /**
         * Normal optimisation would go
         * 1. Break up cascading Selections
         *      We don't need to do this as the limitations of the SJDB has done that for me
         * 2. Push selections down the tree to be below relevant cross products
         * 3. We have selections followed by cross products, we can turn them into Joins
         */

        Node optimisedTree = tree;
        pushSelectDown(optimisedTree);
        return tree;
    }

    private void pushSelectDown(Node tree) {
        if (tree == null) return;

        // Traverse the tree depth first
        pushSelectDown(tree.getLeftChild());
        if(tree.getRightChild() != null) pushSelectDown(tree.getRightChild());

        // Check if current node is a select
        if(Objects.equals(tree.getQueryType(), "Select")) {
            List<Node> scans = findCorrespondingNode(tree.getAttributes(), tree.getLeftChild(), new ArrayList<Node>());
            // If length of scans > 1 then we need to create the subtree
            // because a Select only ever has a max of two fields, can assume there will only ever be a max of 2 scans
            Node child = new Node();

            // Move the node down the tree
        }
    }

    private List<Node> findCorrespondingNode(List<Attribute> attrs, Node current, List<Node> scans) {
        // In order to find the corresponding node to set as child
        // The following condition must be met:
        //      - The child must contain all of the attributes in accordance to the Selection
        //      - if no scans contain relations that have all of the attributes, we need to place a product above the
        //        two relevant scans
        if (current == null) {
            return scans;
        }
        // Check if the relation contains any of the attributes that we want
        if(Objects.equals(current.getQueryType(), "Scan")) {
            for(Attribute a : attrs) {
                if (current.getRel().getAttributes().contains(a)) {
                    scans.add(current);
                    break;
                }
            }
        }

        scans = findCorrespondingNode(attrs, current.getLeftChild(), scans);
        scans = findCorrespondingNode(attrs, current.getRightChild(), scans);

        return scans;
    }

    // Why do you exist
    private Operator reconstructQuery(Node tree) {
        return reconstructQueryRecurse(tree, null);
    }

    private Operator reconstructQueryRecurse(Node tree, Operator query) {
        // Can probably reconstruct using depth first traversal, turning each node into its respective operator and
        // recursively creating its child operator
        if(tree == null) return query;
        switch (tree.getQueryType()) {
            case "Project" -> {
                query = new Project(reconstructQueryRecurse(tree.getLeftChild(), query), tree.getAttributes());
            }
            case "Select" -> {
                // reconstruct predicate
                Predicate pred;
                if (tree.getData() == null) {
                    // if null then attr=attr
                    Attribute leftAttr = tree.getAttributes().get(0);
                    Attribute rightAttr = tree.getAttributes().get(1);
                    pred = new Predicate(leftAttr, rightAttr);
                } else {
                    // attr=value
                    Attribute leftAttr = tree.getAttributes().get(0);
                    String value = tree.getData();
                    pred = new Predicate(leftAttr, value);
                }
                query = new Select(reconstructQueryRecurse(tree.getLeftChild(), query), pred);
            }
            case "Product" -> {
                query = new Product(reconstructQueryRecurse(tree.getLeftChild(), query),
                        reconstructQueryRecurse(tree.getRightChild(), query));
            }
            case "Join" -> {
                Attribute leftAttr = tree.getAttributes().get(0);
                Predicate pred;
                if (tree.getData() == null) {
                    // if null then attr=attr
                    Attribute rightAttr = tree.getAttributes().get(1);
                    pred = new Predicate(leftAttr, rightAttr);
                } else {
                    // attr=value
                    String value = tree.getData();
                    pred = new Predicate(leftAttr, value);
                }
                query = new Join(reconstructQueryRecurse(tree.getLeftChild(), query),
                        reconstructQueryRecurse(tree.getRightChild(), query), pred);
            }
            case "Scan" -> {
                NamedRelation rel = tree.getRel();
                query = new Scan(rel);
            }
        }
        return query;
    }

    private Node constructQueryTree(Operator plan) {
        /**
         * Construct a query tree from the plan
         * We will optimise via reordering of the tree
         * We can reconstruct a query from the optimised tree
         *
         * Leaves represent the tables
         * Nodes represent the operations
         * Root is presumably the final project operation
         */
        Node root = new Node();
        planTraversal(plan, root);
        return root;
    }

    // Recursive method to traverse the query plan and create the query tree
    private Node planTraversal(Operator plan, Node node) {
        try {
            node.setQueryType(plan.getClass().getSimpleName());
            // Hard coded nonsense switch statement my beloved
            switch (plan.getClass().getSimpleName()) {
                case "Project" -> {
                    Project query = (Project) plan;
                    List<Attribute> attributes = query.getAttributes();
                    node.setAttributes(attributes);
                }
                case "Select" -> {
                    Select query = (Select) plan;
                    List<Attribute> attrs = new ArrayList<>();
                    String data = null;
                    attrs.add(query.getPredicate().getLeftAttribute());
                    if (query.getPredicate().equalsValue()) {
                        data = query.getPredicate().getRightValue();
                    } else {
                        attrs.add(query.getPredicate().getRightAttribute());
                    }
                    node.setAttributes(attrs);
                    node.setData(data);
                }
                case "Product" -> {
                    Product query = (Product) plan;
                    node.setAttributes(new ArrayList<Attribute>());
                }
                case "Join" -> {
                    Join query = (Join) plan;
                }
                default -> {
                    Scan query = (Scan) plan;
                    NamedRelation rel = (NamedRelation) query.getRelation();
                    node.setRel(rel);
                }
            }
            if(plan.getInputs().size() == 1) {
                node.setLeftChild(planTraversal(plan.getInputs().get(0), new Node()));
            } else {
                node.setLeftChild(planTraversal(plan.getInputs().get(0), new Node()));
                node.setRightChild(planTraversal(plan.getInputs().get(1), new Node()));
            }
        } catch (Exception e) {
            return node;
        }
        return node;
    }

    // Helper function to print out tree for debugging purposes
    public void depthFirstTraversal(Node root) {
        if (root != null) {
            System.out.println(root); // Process the current node

            // Recursively traverse the left subtree
            depthFirstTraversal(root.getLeftChild());

            // Recursively traverse the right subtree
            depthFirstTraversal(root.getRightChild());
        }
    }
}

class Node{
    // String to represent the type of query the node represents
    private String queryType;
    // List of attributes the query acts upon
    private List<Attribute> attributes;
    // Holds any extra data such as attribute value that may be needed
    private String data = null;
    private NamedRelation rel = null;

    // Child nodes
    private Node leftChild;
    private Node rightChild;

    public void setLeftChild(Node leftChild) {this.leftChild = leftChild;}
    public void setRightChild(Node rightChild) {this.rightChild = rightChild;}
    public Node getLeftChild() {return leftChild;}
    public Node getRightChild() {return rightChild;}

    public void setQueryType(String query) {this.queryType = query;}
    public String getQueryType() {return this.queryType;}
    public void setAttributes(List<Attribute> data) {this.attributes = data;}
    public List<Attribute> getAttributes() {return attributes;}

    @Override
    public String toString() {
        return this.queryType + ": " + this.attributes + ", " + this.data + ", " + this.rel;
    }

    public String getData() {return data;}
    public void setData(String data) {this.data = data;}

    // Used only for scan, this node class is getting uglier and uglier
    public NamedRelation getRel() {return rel;}
    public void setRel(NamedRelation rel) {this.rel = rel;}
}