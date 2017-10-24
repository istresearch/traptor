import six


class RuleSet(object):
    """
    Rule sets provide a convenient data structure to work with lists of
    collection rules.

    The rule set class enables operations using set operations (union,
    difference, intersection) and simple syntax for addition and subtraction
    using the +/- operators.

    Rule sets also maintain an internal index of rules by their value, to help
    optimize the assignment and distribution of collection rules.
    """

    # Dictionary of rule_id -> rule
    rules = None

    # Unique set of rule IDs
    rule_ids = None

    # Dictionary of rule value -> dictionary rule_id -> rule
    rules_by_value = None

    def __init__(self):

        self.rules = dict()
        self.rule_ids = set()
        self.rules_by_value = dict()

    def append(self, rule):
        """
        Adds a collection rule to this rule set.

        :param rule: Collection rule
        :type rule: dict[str, object]
        """

        rule_id = rule.get('rule_id')
        rule_value = self.get_normalized_value(rule)

        if rule_id is None or rule_value is None:
            raise ValueError("The provided Cooper rule is missing rule id or value.")

        self.rules[rule_id] = rule
        self.rule_ids.add(rule_id)

        if rule_value not in self.rules_by_value:
            self.rules_by_value[rule_value] = dict()

        self.rules_by_value[rule_value][rule_id] = rule

    def remove(self, rule_id):
        """
        Removes a collection rule from this rule set.
        :param rule_id: ID of a collection rule
        :type rule_id: str
        :return: The rule that was removed
        :rtype: dict[str, object]
        """
        rule = None
        rule_value = None

        if rule_id in self.rules:
            rule = self.rules[rule_id]
            rule_value = self.get_normalized_value(self.rules[rule_id])
            del self.rules[rule_id]

        if rule_id in self.rule_ids:
            self.rule_ids.remove(rule_id)

        if rule_value is not None and rule_value in self.rules_by_value:

            if rule_id in self.rules_by_value[rule_value]:
                del self.rules_by_value[rule_value][rule_id]

                if len(self.rules_by_value[rule_value]) == 0:
                    del self.rules_by_value[rule_value]

        return rule

    def union(self, other):
        """
        Performs a union (addition) set operation, returning a new RuleSet
        instance containing the combined unique set of elements from both
        RuleSets.

        :param other: Another RuleSet instance
        :type other: RuleSet
        :return: A new RuleSet containing elements of both RuleSets
        :rtype: RuleSet
        """
        rs = RuleSet()

        for rule in six.itervalues(self.rules):
            rs.append(rule)

        for rule in six.itervalues(other.rules):
            rs.append(rule)

        return rs

    def difference(self, other):
        """
        Performs a difference (subtraction) set operation, returning a new
        RuleSet containing the elements of this RuleSet which are not in the
        other RuleSet.

        :param other: Another RuleSet instance
        :type other: RuleSet
        :return: A new RuleSet
        :rtype: RuleSet
        """
        rs = RuleSet()

        rule_ids = self.rule_ids - other.rule_ids

        for rule_id in rule_ids:
            rs.append(self.rules[rule_id])

        return rs

    def intersection(self, other):
        """
        Performs an intersection set operation, returning a new RuleSet that
        contains the unique set of elements that both RuleSets have in common.

        :param other: Another RuleSet instance
        :type other: RuleSet
        :return: A new RuleSet
        :rtype: RuleSet
        """
        return (self + other) - ((self - other) + (other - self))

    def add_local(self, other):
        """
        Adds the elements of the other set to this set, updating this RuleSet
        instance to contain the combined unique set of elements from both
        RuleSets.

        :param other: Another RuleSet instance
        :type other: RuleSet
        """
        for rule in other:
            self.append(rule)

    def subtract_local(self, other):
        """
        Removes the elements of the other set from this set.

        :param other: Another RuleSet instance
        :type other: RuleSet
        """
        for rule in other:
            self.remove(rule.get('rule_id'))

    def get_normalized_value(self, rule):
        """
        Normalize the rule value to ensure correct assignment.
        :param rule: A rule dictionary.
        :type rule: dict
        :return: The normalized value of the rule.
        :rtype: str
        """
        value = rule.get('value')

        if value is not None:
            if 'orig_type' in rule and rule['orig_type'] is not None:
                rule_type = rule['orig_type']

                # For hastag rules ensure each term starts with '#' to
                # distinguish from keyword and prevent over-collection
                if rule_type == 'hashtag':
                    tokens = []
                    for token in value.split(' '):
                        if token:
                            if not token.startswith('#'):
                                tokens.append('#' + token)
                            else:
                                tokens.append(token)

                    value = ' '.join(tokens)

            # Normalizing the rule value to lower case
            value = value.lower()

        return value

    def __add__(self, other):
        """
        Operator (+) implementation to perform a union operation.
        :param other:
        :return:
        """
        return self.union(other)

    def __sub__(self, other):
        """
        Operator (-) implementation to perform a difference operation.
        :param other:
        :return:
        """
        return self.difference(other)

    def __contains__(self, item):
        """
        Tests if a given item exists in this RuleSet.

        :param item: A collection rule
        :type item: dict[str, object]
        :return: True if the rule is a member of this set, otherwise False.
        """
        if item is not None and isinstance(item, dict) and 'rule_id' in item:
            return item.get('rule_id') in self.rule_ids
        return False

    def __len__(self):
        """
        Returns the count of collection rules in this RuleSet.
        :return:
        """
        return len(self.rule_ids)

    def __iter__(self):
        """
        Returns an iterator of the collection rules in the RuleSet.
        :return:
        """
        return six.itervalues(self.rules)

    def __eq__(self, other):
        """
        Tests for equality between two RuleSet objects. Equality is defined in
        this context as having the same set of rule IDs.
        :param other:
        :return:
        """
        return other is not None and isinstance(other, RuleSet) and \
            self.rule_ids == other.rule_ids

    def __ne__(self, other):
        """
        Tests for inequality between two RuleSet objects. Equality is defined in
        this context as having the same set of rule IDs.
        :param other:
        :return:
        """
        return other is None or not isinstance(other, RuleSet) or \
               self.rule_ids != other.rule_ids

    def __repr__(self):
        return "(rules: {}, values: {})".format(len(self.rule_ids), len(self.rules_by_value))


class ReadOnlyRuleSet(RuleSet):
    """
    A protected RuleSet implementation that disallows addition or removal of
    rules.
    """

    def __init__(self, rule_set):
        """
        The provided RuleSet will continue to back this read-only instance, and
        any changes made to the original RuleSet will be portrayed here.

        :param rule_set: A RuleSet instance to back this
        :type rule_set: RuleSet
        """
        super(ReadOnlyRuleSet, self).__init__()

        self.rules = rule_set.rules
        self.rule_ids = rule_set.rule_ids
        self.rules_by_value = rule_set.rules_by_value

    def append(self, rule):
        raise NotImplementedError("Appending rules is not permitted on a ReadOnlyRuleSet.")

    def remove(self, rule_id):
        raise NotImplementedError("Removing rules is not permitted on a ReadOnlyRuleSet.")
