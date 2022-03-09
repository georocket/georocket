package io.georocket.util

import io.georocket.util.PathUtils.join
import io.georocket.util.PathUtils.normalize
import io.georocket.util.PathUtils.removeLeadingSlash
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Test [PathUtils]
 * @author Andrej Sajenko
 */
class PathUtilsTest {
    /**
     * Test join with preceding and descending slashes
     */
    @Test
    fun plainJoin() {
        assertEquals("abc", join("abc"))
        assertEquals("aa/bb", join("aa", "bb"))
        assertEquals("/aa/bb", join("/aa", "bb"))
        assertEquals("/aa/bb", join("/aa", "/bb"))
        assertEquals("/aa/bb/", join("/aa", "/bb/"))
    }

    /**
     * Test join with null and empty arguments
     */
    @Test
    fun emptyArgumentsJoin() {
        assertEquals("", join())
        assertEquals("aa", join("aa", null))
        assertEquals("aa/bb", join("aa", "", "bb"))
    }

    /**
     * Test join normalization
     */
    @Test
    fun normalizeJoin() {
        assertEquals("bb", join("./aa", "../bb"))
        assertEquals("aa/bb", join("./aa", "./bb"))
    }

    /**
     * Test normalize on double slashes
     */
    @Test
    fun normalizeNormalizedPaths() {
        assertEquals("abc", normalize("abc"))
        assertEquals("/abc/abc", normalize("/abc/abc"))
    }

    /**
     * Test normalize on double slashes
     */
    @Test
    fun testNormalize() {
        assertEquals("/abc", normalize("//abc"))
        assertEquals("/abc/abc/", normalize("//abc//abc//"))
        assertEquals("/abc/", normalize("/abc/"))
    }

    /**
     * Test normalize with relative in out paths.
     */
    @Test
    fun relativeInOutNormalize() {
        assertEquals("abc", normalize("./abc"))
        assertEquals("../abc", normalize("../abc"))
        assertEquals("../../abc", normalize("../../abc"))
        assertEquals("bb", normalize("./abc/../bb"))
        assertEquals("", normalize("./abc/../"))
    }

    /**
     * Remove leading slash
     */
    @Test
    fun testRemoveLeadingSlash() {
        assertEquals("abc", removeLeadingSlash("/abc"))
        assertEquals("abc/abc", removeLeadingSlash("/abc/abc"))
        val pathWithoutLeadingSlash = "abc/abc"
        assertEquals(pathWithoutLeadingSlash, removeLeadingSlash(pathWithoutLeadingSlash))
    }

}
