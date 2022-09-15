from dataclasses import dataclass, field
from typing import Any, Dict, Generator


@dataclass
class Node:
    children: Dict[str, "Node"] = field(default_factory=dict)
    content: Any = None


class MQTTMatcher:
    """
    Intended to manage topic filters including wildcards.

    Internally, MQTTMatcher use a prefix tree (trie) to store
    values associated with filters, and has an iter_match()
    method to iterate efficiently over all filters that match
    some topic name.
    """
    _root: Node

    def __init__(self):
        self._root = Node()

    def __setitem__(self, key: str, value: Any):
        """
        Add a topic filter :key to the prefix tree and associate it to :value
        """
        node = self._root
        for sym in key.split("/"):
            node = node.children.setdefault(sym, Node())
        node.content = value

    def __getitem__(self, key: str) -> Any:
        """
        Retrieve the value associated with some topic filter :key
        """
        try:
            node = self._root
            for sym in key.split("/"):
                node = node.children[sym]
            if node.content is None:
                raise KeyError(key)
            return node.content
        except KeyError:
            raise KeyError(key)

    def __delitem__(self, key: str):
        """
        Delete the value associated with some topic filter :key
        """
        lst = []
        try:
            parent, node = None, self._root
            for k in key.split("/"):
                parent, node = node, node.children[k]
                lst.append((parent, k, node))
            # TODO
            node.content = None
        except KeyError:
            raise KeyError(key)
        else:  # cleanup
            for parent, k, node in reversed(lst):
                if node.children or node.content is not None:
                    break
                del parent.children[k]

    def iter_match(self, topic: str) -> Generator[Any, None, None]:
        """
        Return an iterator on all values associated with filters that match the :topic
        """
        lst = topic.split("/")
        normal = not topic.startswith("$")

        def rec(node: Node, i: int = 0) -> Generator[Any, None, None]:
            if i == len(lst):
                if node.content is not None:
                    yield node.content
            else:
                part = lst[i]
                if part in node.children:
                    for content in rec(node.children[part], i + 1):
                        yield content
                if "+" in node.children and (normal or i > 0):
                    for content in rec(node.children["+"], i + 1):
                        yield content
            if "#" in node.children and (normal or i > 0):
                content = node.children["#"].content
                if content is not None:
                    yield content
        return rec(self._root)
